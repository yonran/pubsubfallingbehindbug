package io.github.yonran.pubsubfallingbehindbug;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.gax.rpc.NoHeaderProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.StreamingPullRequest;
import com.google.pubsub.v1.StreamingPullResponse;
import com.google.pubsub.v1.SubscriberGrpc;
import io.github.yonran.pubsubfallingbehindbug.schema.PubsubMessageMetadata;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import picocli.CommandLine;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@CommandLine.Command(name = "grpc", mixinStandardHelpOptions = true, version = "v0.0.0")
public class RawPubSub implements Runnable {
	@CommandLine.Option(names = {"--project"}, required = false)
	String project;

	// For Publisher
	@CommandLine.Option(names = {"--topic"}, required = false,
			description = "(Publisher) Topic to publish to. If non-null, then we will create a publisher.")
	private String topic;

	@CommandLine.Option(names = {"--initial-publish-messages"}, required = false,
			description = "(Publisher) Number of messages to publish when the publisher is created.")
	private int initialPublishMessgages = 0;

	@CommandLine.Option(names = {"--publish-period"}, required = false,
			description = "(Publisher) Number of messages to publish when the publisher is created.")
	private long sleepBetweenPublish = (long)(1000/2.5);


	// For Subscription
	@CommandLine.Option(names = {"--subscription"}, required = true)
	private String subscription;

	@CommandLine.Option(names = {"--log-to"}, required = false)
	private File logTo;

	@CommandLine.Option(names = {"--message-processing-time"}, required = false,
			description = "(Subscriber) Amount of time that each message takes. This should not affect the receiver throughput " +
					"as long as message-processing-time < processor rate * concurrent-messages")
	private long perMessageSleepMs = 5000;

	@CommandLine.Option(names = {"--period"}, required = false,
			description = "(Subscriber) Amount of time between messages that complete processing among all receivers." +
					"This allows you to tune the receiver rate.")
	private long minTimeBetweenMessagesMs = 1000/3;

	private static final Logger logger = LoggerFactory.getLogger(RawPubSub.class);

	private static final long DEADLINE_EXTEND_PADDING = 5000;
	private static final long INITIAL_ASSUMED_DEADLINE = 10000;
	private static final long DEADLINE_EXTEND_AMOUNT = 20000;
	// Copied from Subscriber.java:
	private static final int MAX_INBOUND_MESSAGE_SIZE = 20 * 1024 * 1024; // 20MB API maximum message size.
	private static final int THREADS_PER_CHANNEL = 5;
	private static final ExecutorProvider DEFAULT_EXECUTOR_PROVIDER =
			InstantiatingExecutorProvider.newBuilder()
					.setExecutorThreadCount(
							THREADS_PER_CHANNEL * Runtime.getRuntime().availableProcessors())
					.build();
	private ScheduledExecutorService executor;
	private static class MessageAndTime {
		private final ReceivedMessage receivedMessage;
		private final long minAckTime;
		private final long deadlineExtendTime;
		public MessageAndTime(ReceivedMessage receivedMessage, long minAckTime, long deadlineExtendTime) {
			this.receivedMessage = receivedMessage;
			this.minAckTime = minAckTime;
			this.deadlineExtendTime = deadlineExtendTime;
		}
	}

	@Override
	public void run() {
		if (project == null) {
			// get from GOOGLE_CLOUD_PROJECT, service account, ~/.gcloud/config, or instance metadata
			project = ServiceOptions.getDefaultProjectId();
			if (project == null) {
				System.err.println("You must specify --project or set up application-default project");
			}
		}
		ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(project, subscription);
		if (topic != null) {
			MyProducer producer = null;
			try {
				producer = new MyProducer(ProjectTopicName.of(project, topic),
						initialPublishMessgages,
						sleepBetweenPublish);
			} catch (IOException e) {
				logger.error("Could not create pubsub producer", e);
				System.exit(1);
			}
			Thread thread = new Thread(producer, "Producer thread");
			thread.setDaemon(true);
			thread.start();
		}


		// Based on Subscriber.java:
		TransportChannelProvider channelProvider = SubscriptionAdminSettings.defaultGrpcTransportProviderBuilder()
				.setMaxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
				.setKeepAliveTime(Duration.ofMinutes(5))
				.build();
		if (channelProvider.needsExecutor()) {
			executor = DEFAULT_EXECUTOR_PROVIDER.getExecutor();
			channelProvider = channelProvider.withExecutor(executor);
		}
		HeaderProvider headerProvider = new NoHeaderProvider();
		HeaderProvider internalHeaderProvider = new NoHeaderProvider();
		if (channelProvider.needsHeaders()) {
			Map<String, String> headers =
					ImmutableMap.<String, String>builder()
							.putAll(headerProvider.getHeaders())
							.putAll(internalHeaderProvider.getHeaders())
							.build();
			channelProvider = channelProvider.withHeaders(headers);
		}
		if (channelProvider.needsEndpoint()) {
			channelProvider = channelProvider.withEndpoint(SubscriptionAdminSettings.getDefaultEndpoint());
		}
		GoogleCredentialsProvider credentialsProvider = SubscriptionAdminSettings.defaultCredentialsProviderBuilder().build();
		Credentials credentials;
		try {
			credentials = credentialsProvider.getCredentials();
		} catch (IOException e) {
			logger.error("Could not get credentials", e);
			return;
		}
		CallCredentials callCredentials = credentials == null ? null : MoreCallCredentials.from(credentials);
		GrpcTransportChannel transportChannel;
		Channel channel;
		try {
			// Create only a single Channel
			transportChannel = (GrpcTransportChannel) channelProvider.getTransportChannel();
			channel = transportChannel.getChannel();
		} catch (IOException e) {
			logger.error("Failure creating channel", e);
			return;
		}
		// Synchronized on itself
		LinkedList<MessageAndTime> pendingMessages = new LinkedList<>();
		SettableFuture<Void> complete = SettableFuture.create();
		try {
			// google-cloud-pubsub/src/main/java/com/google/cloud/pubsub/v1/StreamingSubscriberConnection.java
			SubscriberGrpc.SubscriberStub stubTmp = SubscriberGrpc.newStub(channel);
			if (callCredentials != null) stubTmp = stubTmp.withCallCredentials(callCredentials);
			final SubscriberGrpc.SubscriberStub stub = stubTmp;
			// based on com.google.cloud.pubsub.v1.StreamingSubscriberConnection.StreamingPullResponseObserver
			StreamObserver<StreamingPullResponse> responseObserver = new ClientResponseObserver<StreamingPullRequest, StreamingPullResponse>() {
				@Override
				public void beforeStart(ClientCallStreamObserver<StreamingPullRequest> requestObserver) {
					logger.trace("ResponseObserver disabling flow control");
					requestObserver.disableAutoInboundFlowControl();
				}

				@Override
				public void onNext(StreamingPullResponse response) {
					List<ReceivedMessage> receivedMessages = response.getReceivedMessagesList();
					long now = System.currentTimeMillis();
					List<String> messageIds = new ArrayList<>();
					for (ReceivedMessage receivedMessage: receivedMessages) {
						messageIds.add(receivedMessage.getMessage().getMessageId());
					}
					logger.info("Received " + receivedMessages.size() + " messages: " + messageIds);
					synchronized (pendingMessages) {
						for (ReceivedMessage receivedMessage: receivedMessages) {
							pendingMessages.add(new MessageAndTime(
									receivedMessage,
									now + perMessageSleepMs,
									now + INITIAL_ASSUMED_DEADLINE - DEADLINE_EXTEND_PADDING
							));
						}
						pendingMessages.notifyAll();
					}
				}

				@Override
				public void onError(Throwable t) {
					complete.setException(t);
				}

				@Override
				public void onCompleted() {
					complete.set(null);
				}
			};
			ClientCallStreamObserver<StreamingPullRequest> requestObserver =
					(ClientCallStreamObserver<StreamingPullRequest>)
							stub.streamingPull(responseObserver);
			// We need to set streaming ack deadline, but it's not useful since we'll modack to send receipt anyway.
			// Set to some big-ish value in case we modack late.
			requestObserver.onNext(
					StreamingPullRequest.newBuilder()
							.setSubscription(subscriptionName.toString())
							.setStreamAckDeadlineSeconds(60)
							.build());
			requestObserver.request(1);

			Thread receiverThread = new Thread(new Runnable() {
				@Override
				public void run() {
					logger.debug("Receiver thread starting");
					try {
						ObjectMapper mapper = new ObjectMapper();
						OutputStream outputStream;
						if (logTo != null) {
							outputStream = new FileOutputStream(logTo);
						} else {
							outputStream = ByteStreams.nullOutputStream();
						}
						JsonGenerator generator;
						try {
							generator = mapper.getFactory()
									.setRootValueSeparator("\n")
									.createGenerator(outputStream);
						} catch (IOException e) {
							throw new RuntimeException(e);  // should not happen
						}

						SubscriberGrpc.SubscriberStub timeoutStub =
								stub.withDeadlineAfter(60, TimeUnit.SECONDS);

						long nextAckTime = 0;  // Minimum ack time based on simulated processing time
						while (true) {
							long now = System.currentTimeMillis();
							long newDeadline = now + DEADLINE_EXTEND_AMOUNT;
							long newDeadlineExtendTime = newDeadline - DEADLINE_EXTEND_PADDING;
							int deadlineDurationSeconds = (int) ((newDeadline - now) / 1000);
							ReceivedMessage messageToAck = null;
							ArrayList<ReceivedMessage> messagesToExtend = new ArrayList<>();

							synchronized (pendingMessages) {
								if (pendingMessages.size() < 1) {
									int count = 1;
									logger.trace("Receiver requesting " + count + " message");
									requestObserver.request(count);
								}
								if (pendingMessages.isEmpty()) {
									logger.trace("Receiver thread queue empty; waiting");
									pendingMessages.wait();
									continue;
								} else {
									long nextExtendTime = Long.MAX_VALUE;
									for (MessageAndTime messageAndTime: pendingMessages) {
										nextExtendTime = Math.min(nextExtendTime, messageAndTime.deadlineExtendTime);
									}
									MessageAndTime nextMessage = pendingMessages.getFirst();
									long nextMessageAckTime = Math.max(
											nextAckTime,
											nextMessage.minAckTime);
									long waitTime = Math.min(nextMessageAckTime, nextExtendTime) - now;
									if (waitTime > 0) {
										logger.trace("Receiver thread waiting up to " + waitTime + "ms for something interesting (next ack: " +
												(nextMessageAckTime - now) + "ms; next modack: " + (nextExtendTime - now) + ")");
										pendingMessages.wait(waitTime);
										continue;
									} else if (nextMessageAckTime <= now) {
										logger.trace("Receiver thread popping 1 message to ack");
										messageToAck = pendingMessages.removeFirst().receivedMessage;
									} else if (nextExtendTime <= now) {
										for (ListIterator<MessageAndTime> it = pendingMessages.listIterator(); it.hasNext();) {
											MessageAndTime messageAndTime = it.next();
											if (messageAndTime.deadlineExtendTime <= now) {
												it.remove();
												it.add(new MessageAndTime(
														messageAndTime.receivedMessage,
														messageAndTime.minAckTime,
														newDeadlineExtendTime));
												messagesToExtend.add(messageAndTime.receivedMessage);
											}
										}
										logger.trace("Receiver thread extending " + messagesToExtend.size() + " deadlines to " + deadlineDurationSeconds + "s from now");
									} else {
										throw new IllegalStateException("nextActTime or nextExtendTime should have happened if waitTime == 0");
									}
								}
							}
							boolean didSomething = false;
							if (messageToAck != null) {
								// based on com.google.cloud.pubsub.v1.StreamingSubscriberConnection.sendAckOperations
								String ackId = messageToAck.getAckId();
								String messageId = messageToAck.getMessage().getMessageId();
								StreamObserver<Empty> loggingObserver = new StreamObserver<Empty>() {
									@Override public void onNext(Empty value) { logger.trace("ack next"); }
									@Override public void onError(Throwable t) {
										logger.error("ack " + messageId + " failed", t);
									}
									@Override public void onCompleted() { logger.trace("ack completed"); }
								};
								logger.debug("Sending 1 ack: " + messageId);
								timeoutStub.acknowledge(
										AcknowledgeRequest.newBuilder()
												.setSubscription(subscriptionName.toString())
												.addAllAckIds(Collections.singletonList(ackId))
												.build(),
										loggingObserver);
								nextAckTime = now + minTimeBetweenMessagesMs;

								PubsubMessage message = messageToAck.getMessage();
								Instant nowInstant = Instant.ofEpochMilli(now);
								Timestamp publishTime = message.getPublishTime();
								Instant publishTimeInstant = Instant.ofEpochSecond(publishTime.getSeconds(), publishTime.getNanos());
								PubsubMessageMetadata jsonMsg = new PubsubMessageMetadata(
										message.getMessageId(),
										publishTimeInstant.toString(),
										nowInstant.toString()
								);
								mapper.writeValue(generator, jsonMsg);
								generator.flush();
								didSomething = true;
							}
							if (!messagesToExtend.isEmpty()) {
								// based on com.google.cloud.pubsub.v1.StreamingSubscriberConnection.sendAckOperations
								ArrayList<String> ackIds = new ArrayList<>();
								ArrayList<String> messageIds = new ArrayList<>();
								StreamObserver<Empty> loggingObserver = new StreamObserver<Empty>() {
									@Override public void onNext(Empty value) { logger.trace("extend next"); }
									@Override public void onError(Throwable t) {
										logger.error("extend " + messageIds + " failed", t);
									}
									@Override public void onCompleted() { logger.trace("extend completed"); }
								};

								for (ReceivedMessage receivedMessage: messagesToExtend) {
									ackIds.add(receivedMessage.getAckId());
									messageIds.add(receivedMessage.getMessage().getMessageId());
								}
								logger.debug("Sending " + messageIds.size() + " modacks to " + deadlineDurationSeconds + ": " + messageIds);
								timeoutStub.modifyAckDeadline(
										ModifyAckDeadlineRequest.newBuilder()
												.setSubscription(subscriptionName.toString())
												.addAllAckIds(ackIds)
												.setAckDeadlineSeconds(deadlineDurationSeconds)
												.build(),
										loggingObserver);
								didSomething = true;
							}
							if (!didSomething) {
								throw new IllegalStateException("Critical loop should have given us work to do");
							}
						}
					} catch (Exception e) {
						complete.setException(e);
					}
				}
			}, "MyMessageReceiver");
			receiverThread.setDaemon(true);
			receiverThread.start();

			complete.get();
		} catch (InterruptedException e) {
			logger.error("Interrupted", e);
		} catch (ExecutionException e) {
			logger.error("StreamObserver onError", e);
		} finally {
			if (channelProvider.shouldAutoClose()) {
				try {
					transportChannel.close();
				} catch (Exception e) {
					logger.error("Could not close stream", e);
				}
			}
		}
	}
}

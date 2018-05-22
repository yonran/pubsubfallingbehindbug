package io.github.yonran.pubsubfallingbehindbug;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.FileNotFoundException;

@CommandLine.Command(name = "cloud", mixinStandardHelpOptions = true, version = "v0.0.0", showDefaultValues = true)
public class CloudPubSub implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(CloudPubSub.class);

	@CommandLine.Option(names = {"--project"})
	private String project;

	@CommandLine.Option(names = {"--subscription"}, required = true)
	private String subscription;

	@CommandLine.Option(names = {"--log-to"}, required = false)
	private File logTo;

	@CommandLine.Option(names = {"--concurrent-messages"}, required = false,
			description = "Number of concurrent receiveMessage calls.")
	private long concurrentReceiveCount = 20;

	@CommandLine.Option(names = {"--message-processing-time"}, required = false,
			description = "Amount of time that each message takes. This should not affect the receiver throughput " +
					"as long as message-processing-time < processor rate * concurrent-messages")
	private long perMessageSleepMs = 5000;

	@CommandLine.Option(names = {"--period"}, required = false,
			description = "Amount of time between messages that complete processing among all receivers." +
					"This allows you to tune the receiver rate.")
	private long minTimeBetweenMessagesMs = 1000/3;

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

		try {
			LogMessagesReceiver pubSubReceiver = pubSubReceiver = new LogMessagesReceiver(
					logTo,
					perMessageSleepMs,
					minTimeBetweenMessagesMs
					);
			Subscriber subscriber = Subscriber.newBuilder(subscriptionName, pubSubReceiver)
				.setFlowControlSettings(FlowControlSettings.newBuilder()
					// at most 10 concurrent requests should be out
					.setMaxOutstandingElementCount(concurrentReceiveCount)
					// same as default
					.setMaxOutstandingRequestBytes(Runtime.getRuntime().maxMemory() * 20L / 100L)
					.build()
				)
				.build();
			subscriber.startAsync();
			logger.info("Starting PubSub subscriber");
			subscriber.awaitRunning();
			logger.info("Started PubSub subscriber; awaiting termination");
			subscriber.awaitTerminated();
			logger.info("Terminated");
		} catch (FileNotFoundException e) {
			logger.error("Could not start pubsub receiver", e);
		}
	}
}

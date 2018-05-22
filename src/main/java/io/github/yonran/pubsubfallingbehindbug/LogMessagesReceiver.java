package io.github.yonran.pubsubfallingbehindbug;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.common.io.ByteStreams;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import io.github.yonran.pubsubfallingbehindbug.schema.PubsubMessageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;

public class LogMessagesReceiver implements MessageReceiver {
	private static final Logger logger = LoggerFactory.getLogger(LogMessagesReceiver.class);
	private final long perMessageSleepMs;
	private final long minTimeBetweenMessagesMs;
	private final OutputStream outputStream;
	private final ObjectMapper mapper;
	private final JsonGenerator generator;
	private long lastAckTime = 0;

	public LogMessagesReceiver(File path, long perMessageSleepMs, long minTimeBetweenMessagesMs) throws FileNotFoundException {
		this.perMessageSleepMs = perMessageSleepMs;
		this.minTimeBetweenMessagesMs = minTimeBetweenMessagesMs;
		OutputStream outputStream1;
		mapper = new ObjectMapper();
		if (path != null) {
			outputStream1 = new FileOutputStream(path);
		} else {
			outputStream1 = ByteStreams.nullOutputStream();
		}
		outputStream = outputStream1;
		try {
			generator = mapper.getFactory()
					.setRootValueSeparator("\n")
					.createGenerator(outputStream);
		} catch (IOException e) {
			throw new RuntimeException(e);  // should not happen
		}
	}
	@Override
	public void receiveMessage(PubsubMessage message, AckReplyConsumer ackReplyConsumer) {
		Timestamp publishTime = message.getPublishTime();
		Instant publishTimeInstant = Instant.ofEpochSecond(publishTime.getSeconds(), publishTime.getNanos());
		Instant now = Instant.now();
		PubsubMessageMetadata jsonMsg = new PubsubMessageMetadata(
				message.getMessageId(),
				publishTimeInstant.toString(),
				now.toString()
		);
		try {
			// Extra delay that should not affect throughput
			Thread.sleep(perMessageSleepMs);
			long ackTime;
			synchronized(this) {
				mapper.writeValue(generator, jsonMsg);
				generator.flush();
				lastAckTime = Math.max(lastAckTime + minTimeBetweenMessagesMs, now.toEpochMilli());
				ackTime = lastAckTime;
			}
			Thread.sleep(ackTime - now.toEpochMilli());
			ackReplyConsumer.ack();
		} catch (Exception e) {
			ackReplyConsumer.nack();
			logger.error("receiveMessage exception", e);
		}
	}
}
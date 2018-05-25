package io.github.yonran.pubsubfallingbehindbug;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;

public class MyProducer implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(MyProducer.class);
	private final ProjectTopicName topic;
	private final int initialMessageCount;
	private final long sleepBetweenPublish;
	private final Publisher publisher;

	public MyProducer(ProjectTopicName topic, int initialMessageCount, long sleepBetweenPublish) throws IOException {
		this.topic = topic;
		this.initialMessageCount = initialMessageCount;
		this.sleepBetweenPublish = sleepBetweenPublish;
		this.publisher = Publisher.newBuilder(this.topic).build();
	}

	@Override
	public void run() {
		if (initialMessageCount > 0)
			logger.info("Publisher Creating a backlog of " + initialMessageCount + " messages");
		for (long i = 0; ; i++) {
			if (i == initialMessageCount) {
				logger.info("Publisher done creating backlog; entering normal loop with " + sleepBetweenPublish + "ms sleep between messages");
			} else if (i > initialMessageCount) {
				try {
					Thread.sleep(sleepBetweenPublish);
				} catch (InterruptedException e) {
					logger.error("Publisher thread interrupted; exiting", e);
					return;
				}
			}
			Instant now = Instant.now();
			ApiFuture<String> publishFuture = publisher.publish(PubsubMessage.newBuilder()
					.putAttributes("publisherTime", now.toString())
					.putAttributes("index", Long.toString(i))
					.build());
			publishFuture.addListener(new Runnable() {
				@Override
				public void run() {
					try {
						publishFuture.get();
					} catch (InterruptedException e) {
						logger.error("Interrupted publishing message at " + now, e);
					} catch (ExecutionException e) {
						logger.error("Exception publishing message at " + now, e);
					}
				}
			}, MoreExecutors.directExecutor());
			if (i % 10000 == -1 % 10000) {
				logger.info("Published " + (i+1) + " messages");
			}
		}
	}
}

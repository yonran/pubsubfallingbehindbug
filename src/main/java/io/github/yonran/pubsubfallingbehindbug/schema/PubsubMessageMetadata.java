package io.github.yonran.pubsubfallingbehindbug.schema;

import java.util.Objects;

public class PubsubMessageMetadata {
	private final String messageId;
	private final String publishTime;
	private final String receiveTime;

	public PubsubMessageMetadata(String messageId, String publishTime, String receiveTime) {
		this.messageId = messageId;
		this.publishTime = publishTime;
		this.receiveTime = receiveTime;
	}

	public String getPublishTime() { return publishTime; }
	public String getMessageId() { return messageId; }
	public String getReceiveTime() { return receiveTime; }

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PubsubMessageMetadata that = (PubsubMessageMetadata) o;
		return Objects.equals(messageId, that.messageId) &&
				Objects.equals(publishTime, that.publishTime) &&
				Objects.equals(receiveTime, that.receiveTime);
	}

	@Override
	public int hashCode() {
		return Objects.hash(messageId, publishTime, receiveTime);
	}

	@Override
	public String toString() {
		return "PubsubMessageMetadata{" +
				"messageId='" + messageId + '\'' +
				", publishTime='" + publishTime + '\'' +
				", receiveTime='" + receiveTime + '\'' +
				'}';
	}

}

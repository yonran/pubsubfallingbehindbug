package io.github.yonran.pubsubfallingbehindbug.schema;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

public class PubsubMessageMetadata {
	private final String messageId;
	private final String publishTime;
	private final String receiveTime;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private final Integer reconnect;

	public PubsubMessageMetadata(String messageId, String publishTime, String receiveTime, Integer reconnect) {
		this.messageId = messageId;
		this.publishTime = publishTime;
		this.receiveTime = receiveTime;
		this.reconnect = reconnect;
	}

	public String getPublishTime() { return publishTime; }
	public String getMessageId() { return messageId; }
	public String getReceiveTime() { return receiveTime; }
	public Integer getReconnect() { return reconnect; }

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PubsubMessageMetadata that = (PubsubMessageMetadata) o;
		return Objects.equals(messageId, that.messageId) &&
				Objects.equals(publishTime, that.publishTime) &&
				Objects.equals(receiveTime, that.receiveTime) &&
				Objects.equals(reconnect, that.reconnect);
	}

	@Override
	public int hashCode() {
		return Objects.hash(messageId, publishTime, receiveTime, reconnect);
	}

	@Override
	public String toString() {
		return "PubsubMessageMetadata{" +
				"messageId='" + messageId + '\'' +
				", publishTime='" + publishTime + '\'' +
				", receiveTime='" + receiveTime + '\'' +
				", reconnect=" + reconnect +
				'}';
	}
}

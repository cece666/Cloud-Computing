package common.messages;

import java.time.LocalDateTime;
import java.util.Collection;

public class ClientMessage extends KVMessage {

	public ClientMessage(StatusType status, KeyValue... kvs) {
		this(null, status, kvs);
	}

	public ClientMessage(StatusType status, Collection<KeyValue> kvs) {
		this(null, status, kvs);
	}

	public ClientMessage(String username, StatusType status, KeyValue... kvs) {
		this(username, null, status, kvs);
	}

	public ClientMessage(String username, StatusType status, Collection<KeyValue> kvs) {
		this(username, null, status, kvs);
	}

	public ClientMessage(String username, LocalDateTime timestamp, StatusType status, KeyValue... kvs) {
		super(status, kvs);
		pairs.add(0, new KeyValue("username", username));
		pairs.add(1, new KeyValue("timestamp", timestamp == null ? null : timestamp.toString()));
	}

	public ClientMessage(String username, LocalDateTime timestamp, StatusType status, Collection<KeyValue> kvs) {
		super(status, kvs);
		pairs.add(0, new KeyValue("username", username));
		pairs.add(1, new KeyValue("timestamp", timestamp == null ? null : timestamp.toString()));
	}

	public ClientMessage(StatusType status, int size, byte[] pairsBytes) {
		super(status, size, pairsBytes);
	}

	@Override
	public Source getSource() {
		return Source.CLIENT;
	}

	public String getUserName() {
		return pairs.get(0).value;
	}

	public LocalDateTime getDelTime() {
		return LocalDateTime.parse(pairs.get(1).value);
	}
}

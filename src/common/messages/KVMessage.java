package common.messages;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Stream.of;

import java.util.ArrayList;
import java.util.Collection;

import common.util.MarshallUtils;

public abstract class KVMessage {
	/**
	 * This class handle the KVMessage by getting key,value, status and marshalling,
	 * unmarshalling KVMessage.
	 * 
	 * @author Jiaxi Zhao
	 */

	private final StatusType status;

	protected final ArrayList<KeyValue> pairs;

	/**
	 * 
	 * @param status
	 * @param key
	 * @param value
	 */
	public KVMessage(StatusType status, KeyValue... kvs) {
		this.status = status;
		this.pairs = of(kvs).collect(toCollection(ArrayList::new));
	}

	public KVMessage(StatusType status, Collection<KeyValue> kvs) {
		this.status = status;
		this.pairs = new ArrayList<>(kvs);
	}

	public KVMessage(StatusType status, int size, byte[] kvBytes) {
		this.status = status;
		this.pairs = MarshallUtils.pairsFromBytes(size, kvBytes);
	}

	public String getKey() {
		return getKey(0);
	}

	public String getValue() {
		return getValue(0);
	}

	public String getKey(int i) {
		return pairs.get(i).key;
	}

	public String getValue(int i) {
		return pairs.get(i).value;
	}

	public abstract Source getSource();

	public StatusType getStatus() {
		return status;
	}

	public ArrayList<KeyValue> getPairs() {
		return pairs;
	}
}

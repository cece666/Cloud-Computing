package common.messages;

import java.util.Collection;

public class ECSMessage extends KVMessage {
    public ECSMessage(StatusType status, KeyValue... kvs) {
	super(status, kvs);
    }

    public ECSMessage(StatusType status, Collection<KeyValue> kvs) {
	super(status, kvs);
    }

    public ECSMessage(StatusType status, int size, byte[] pairsBytes) {
	super(status, size, pairsBytes);
    }

    @Override
    public Source getSource() {
	return Source.ECS;
    }
}
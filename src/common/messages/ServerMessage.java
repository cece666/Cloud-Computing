package common.messages;

import java.util.Collection;

public class ServerMessage extends KVMessage {
    public ServerMessage(StatusType status, KeyValue... kvs) {
	super(status, kvs);
    }

    public ServerMessage(StatusType status, Collection<KeyValue> kvs) {
	super(status, kvs);
    }

//    public ServerMessage(StatusType status, String key, String value) {
//	super(status, new KeyValue(key, value));
//    }

    public ServerMessage(StatusType status, int size, byte[] pairsBytes) {
	super(status, size, pairsBytes);
    }

    @Override
    public Source getSource() {
	return Source.SERVER;
    }
}
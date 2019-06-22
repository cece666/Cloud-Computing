package client;

import common.messages.KVMessage;

/**
 * This class return different string depend on message statusType.
 * 
 * @author Jiaxi Zhao
 *
 */

public class ClientFormat {
    public static String formatKVMessage(KVMessage message) {
	if (message == null)
	    return "No connection";

	switch (message.getStatus()) {
	case INFO:
	    return message.getValue();
	case FAIL:
	    return String.format("FAIL: %s", message.getValue());
	case GET_ERROR:
	    return String.format("GET_ERROR: %s", message.getKey());
	case SERVER_WRITE_LOCK:
	    return String.format("SERVER_WRITE_LOCK,PLEASE TRY AGAIN LATER");
	case SERVER_STOPPED:
	    return String.format("server shut down, please connect again");
	default:
	    return String.format("%s:\n%s:%s", message.getStatus().name(), message.getKey(), message.getValue());
	}
    }
}

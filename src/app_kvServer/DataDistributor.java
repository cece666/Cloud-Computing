package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KeyValue;
import common.messages.ServerMessage;
import common.messages.StatusType;
import common.metadata.MDEntry;
import common.util.MarshallUtils;

/**
 * This class handles the communication between the server and another server
 * during moving of the data from one to the other.
 * 
 * @author Aleena Yunus
 * 
 */

public class DataDistributor {
	private Socket socket;
	private static Logger logger = LogManager.getLogger("kvServer");

	public DataDistributor(MDEntry sendTo) throws UnknownHostException, IOException {
		socket = new Socket(sendTo.addressPort.address, sendTo.addressPort.port);
		MarshallUtils.writeToServer(new ServerMessage(StatusType.IDENTIFY, new KeyValue("", null)), socket);
	}

	/**
	 * Moves a single data element
	 * 
	 * @param KeyValue
	 *            a key value pair
	 * @return KVMessage a message to notify the ecs
	 */
	public KVMessage moveData(KeyValue... kv) {
		try {
			MarshallUtils.writeToServer(new ServerMessage(StatusType.MOVE_DATA, kv), socket);
			return MarshallUtils.readFromServer(socket);
		} catch (IllegalStateException | IllegalArgumentException | IOException e) {
			logger.error(e);
			return new ServerMessage(StatusType.PUT_ERROR, kv);
		}
	}

	/**
	 * Moves a collection of data elements
	 * 
	 * @param KeyValue
	 *            a collection of key value pairs
	 * @return KVMessage a message to notify the ecs
	 */
	public KVMessage moveData(Collection<KeyValue> kv) {
		try {
			MarshallUtils.writeToServer(new ServerMessage(StatusType.MOVE_DATA, kv), socket);
			return MarshallUtils.readFromServer(socket);
		} catch (IllegalStateException | IllegalArgumentException | IOException e) {
			logger.error(e);
			return new ServerMessage(StatusType.PUT_ERROR, kv);
		}
	}

	/**
	 * Replicates a single data element
	 * 
	 * @param KeyValue
	 *            a key value pair
	 * @return KVMessage a message to notify the ecs
	 */
	public KVMessage replicateData(KeyValue... kv) {
		try {
			MarshallUtils.writeToServer(new ServerMessage(StatusType.REPLICATE, kv), socket);
			return MarshallUtils.readFromServer(socket);
		} catch (IllegalStateException | IllegalArgumentException | IOException e) {
			logger.error(e);
			return new ServerMessage(StatusType.PUT_ERROR, kv);
		}
	}

	/**
	 * Replicates a collection of data elements
	 * 
	 * @param KeyValue
	 *            a collection of key value pairs
	 * @return KVMessage a message to notify the ecs
	 */
	public KVMessage replicateData(Collection<KeyValue> kv) {
		try {
			MarshallUtils.writeToServer(new ServerMessage(StatusType.REPLICATE, kv), socket);
			return MarshallUtils.readFromServer(socket);
		} catch (IllegalStateException | IllegalArgumentException | IOException e) {
			logger.error(e);
			return new ServerMessage(StatusType.PUT_ERROR, kv);
		}
	}

	/**
	 * Replicates User message
	 * 
	 * @param username
	 * @param password
	 * @return
	 */
	public KVMessage broadcastNewUser(String username, String password) {
		try {
			MarshallUtils.writeToServer(new ServerMessage(StatusType.ADD_USER, new KeyValue(username, password)),
					socket);
			return MarshallUtils.readFromServer(socket);
		} catch (IllegalStateException | IllegalArgumentException | IOException e) {
			logger.error(e);
			return new ServerMessage(StatusType.ADD_USER_ERROR, new KeyValue(username, password));
		}
	}
}

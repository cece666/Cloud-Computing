package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import app_kvDatabase.KVData;
import app_kvDatabase.KVDatabase;
import app_kvDatabase.KVEntity;
import common.hash.Hash;
import common.messages.KVMessage;
import common.messages.KeyValue;
import common.messages.ServerMessage;
import common.messages.StatusType;
import common.metadata.MDEntry;

/**
 * This class handles the server-server communication in the case of data
 * movement
 * 
 * @Author Uy Ha
 */

public class ServerHandler extends BaseHandler {
	private static final Logger logger = LogManager.getLogger("kvServer");
	private final ServerState state;

	public ServerHandler(Socket socket, ServerState state) {
		super(socket, Executors.newCachedThreadPool(), logger);
		this.state = state;
	}

	@Override
	protected void preRun() throws IOException {
	}

	/**
	 * Performs a simple put request for the key value transferred by another server
	 * 
	 * @param message
	 *            the message to be parsed
	 * @return KVMessage
	 */
	@Override
	protected KVMessage directMessage(KVMessage message) {
		switch (message.getStatus()) {
		case MOVE_DATA:
			return moveData(message);
		case PING:
			return Pong();
		case REPLICATE:
			return replicateData(message);
		case ADD_USER:
			return addNewUser(message);
		default:
			throw new IllegalArgumentException("Message's action is not in the allowed methods");
		}
	}

	/**
	 * Saves data sent from another server
	 * 
	 * @param message
	 *            the message to be parsed
	 * @return KVMessage
	 */
	private KVMessage moveData(KVMessage message) {
		ArrayList<KeyValue> pairs = message.getPairs();
		for (int i = 0; i < pairs.size(); i += 3) {
			String key = pairs.get(i).key;
			String value = pairs.get(i).value;
			String owner = pairs.get(i + 1).value;
			LocalDateTime delTime = LocalDateTime.parse(pairs.get(i + 2).value);
			state.getDb().put(key, new KVData(value, owner, delTime));
		}
		return new ServerMessage(StatusType.MOVE_DATA_SUCCESS, new KeyValue());
	}

	/**
	 * This method add ner user to it's user database
	 * 
	 * @param message
	 * @return
	 */
	private KVMessage addNewUser(KVMessage message) {
		String username = message.getKey();
		if (!state.getUserDb().exists(username)) {
			try {
				state.getUserDb().put(message.getKey(), message.getKey());
				return new ServerMessage(StatusType.ADD_USER_SUCCESS, new KeyValue("", message.getKey()));
			} catch (IOException e) {
				logger.warn(e);
				return new ServerMessage(StatusType.ADD_USER_ERROR,
						new KeyValue("", "Failed to create user due to internal error"));
			}
		} else {
			return new ServerMessage(StatusType.ADD_USER_ERROR, new KeyValue(message.getKey(), "User already exists"));
		}

	}

	/**
	 * Replicates data sent from another server
	 * 
	 * @param message
	 *            the message to be parsed
	 * @return KVMessage
	 */
	private KVMessage replicateData(KVMessage message) {
		ArrayList<KeyValue> pairs = message.getPairs();
		if (!pairs.isEmpty()) {
			KVDatabase replica = redirectTo(pairs.get(0).key);

			ArrayList<KVEntity> entities = new ArrayList<>();
			for (int i = 1; i < pairs.size(); i += 3) {
				String key = pairs.get(i).key;
				String value = pairs.get(i).value;
				String owner = pairs.get(i + 1).value;
				LocalDateTime delTime = LocalDateTime.parse(pairs.get(i + 2).value);
				entities.add(new KVEntity(key, new KVData(value, owner, delTime)));
			}

			replica.replicate(entities);
		}
		return new ServerMessage(StatusType.REPLICATE_SUCCESS, new KeyValue());
	}

	/**
	 * Replies to a ping
	 * 
	 * @param Nothing
	 * @return KVMessage
	 */
	private KVMessage Pong() {
		return new ServerMessage(StatusType.PONG, new KeyValue());
	}

	/**
	 * Determines which replica to store the data to
	 * 
	 * @param firstKey
	 *            first key of the sent data elems
	 * @return Database the db to redirect to
	 */
	private KVDatabase redirectTo(String firstKey) {
		byte[] keyHash = Hash.hash(firstKey);
		MDEntry pred = state.getMetadata().getPredecessor(state.getServerMeta());
		MDEntry predPred = state.getMetadata().getPredecessor(pred);
		if (Hash.in(keyHash, predPred.hashIndex, pred.hashIndex)) {
			return state.getReplica1();
		} else {
			return state.getReplica2();
		}
	}

}

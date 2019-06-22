package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import app_kvDatabase.KVData;
import app_kvServer.KVServer.Status;
import common.hash.Hash;
import common.messages.KVMessage;
import common.messages.KeyValue;
import common.messages.ServerMessage;
import common.messages.StatusType;
import common.metadata.MDEntry;
import common.util.MarshallUtils;

/**
 * This class handles the communication between the server and a client, it
 * redirects all the requests to the appropriate handlers a send back the
 * response to client
 * 
 * @author Uy Ha
 * 
 */
public class ClientHandler extends BaseHandler {
	private static final int MAX_KEY_LENGTH = 20;
	private static final int MAX_VALUE_LENGTH = 120 * (1 << 10);
	private static Logger logger = LogManager.getLogger("kvServer");
	private final ServerState state;

	/**
	 * 
	 * @param socket
	 * @param db
	 * @param service
	 * @param cache
	 */
	public ClientHandler(Socket socket, ServerState state) {
		super(socket, Executors.newCachedThreadPool(), logger);
		this.state = state;
	}

	/*
	 * This method run before processing socket message, sending latest meta table
	 * to client. (non-Javadoc)
	 * 
	 * @see app_kvServer.BaseHandler#preRun()
	 */
	protected void preRun() throws IOException {
		try {
			String connectionResponse = String.format("Connected to %s", socket.getInetAddress().getHostName());
			String metaString = state.getMetadata() == null ? "" : state.getMetadata().toMessageValue();
			MarshallUtils.writeToServer(new ServerMessage(StatusType.INFO, new KeyValue("response", connectionResponse),
					new KeyValue("meta", metaString)), socket);
		} catch (Exception e) {
			logger.error(e);
		}
	}

	/**
	 * Give a callable that calls the appropriate method based on the action in the
	 * given message
	 * 
	 * @param message
	 *            the encoded KVMessage
	 * @return a callable that calls the appropriate function
	 */
	protected KVMessage directMessage(KVMessage message) {
		if (!readyForNormalClient()) {
			return new ServerMessage(StatusType.SERVER_STOPPED,
					new KeyValue(message.getKey(), "Server is stopped, no requests are processed"));
		}

		if (message.getStatus() == StatusType.GET) {
			if (message.getValue(0) == null) {
				return new ServerMessage(StatusType.FAIL,
						new KeyValue(message.getKey(), "Login or signup first, please!"));
			}
			return get(message.getValue(0), message.getKey(2));
		}

		if (!isResponsible(Hash.hash(message.getKey()))) {
			return new ServerMessage(StatusType.SERVER_NOT_RESPONSIBLE,
					new KeyValue(message.getKey(2), "Server is not responsible"),
					new KeyValue("meta", state.getMetadata().toMessageValue()));
		}

		switch (message.getStatus()) {
		case TIMED_PUT:
			if (message.getValue(0) == null) {
				return new ServerMessage(StatusType.FAIL,
						new KeyValue(message.getKey(), "Login or signup first, please!"));
			}
			return timedPut(message.getValue(0), message.getValue(1), message.getKey(2), message.getValue(2));
		case LOGIN:
			if (message.getValue(2) == null) {
				return new ServerMessage(StatusType.LOGIN_ERROR,
						new KeyValue(message.getKey(2), "Password cannot be empty"));
			}
			return login(message.getKey(2), message.getValue(2));
		case SIGN_UP:
			if (message.getValue(2) == null) {
				return new ServerMessage(StatusType.SIGN_UP_ERROR,
						new KeyValue(message.getKey(2), "Password cannot be empty"));
			}
			return signup(message.getKey(2), message.getValue(2));
		case PUT:
			if (message.getValue(0) == null) {
				return new ServerMessage(StatusType.FAIL,
						new KeyValue(message.getKey(), "Login or signup first, please!"));
			}
			return put(message.getValue(0), message.getKey(2), message.getValue(2));
		case DELETE:
			if (message.getValue(0) == null) {
				return new ServerMessage(StatusType.FAIL,
						new KeyValue(message.getKey(), "Login or signup first, please!"));
			}
			return delete(message.getValue(0), message.getKey(2));
		default:
			throw new IllegalArgumentException("Message's action is not in the allowed methods");
		}
	}

	/**
	 * Get the pair with the associated key
	 * 
	 * @param key
	 * @return return a {@code GET_SUCCESS} message if the pair with the given key
	 *         exists in the database, otherwise return a {@code GET_ERROR} with
	 *         value is null if no pair with the given key exists, or error message
	 *         when something happened when reading from the database.
	 */
	private KVMessage get(String owner, String key) {

		if (isResponsibleforGet(Hash.hash(key))) {
			String value = null;
			String cachedValue = state.getCache().get(key, owner);
			if (cachedValue != null) {
				value = cachedValue;
			} else if (state.getDb().get(key) != null && state.getDb().get(key).owner == owner) {
				value = state.getDb().get(key).value;
			} else if (state.getDb().get(key) != null && state.getDb().get(key).owner != owner) {
				return new ServerMessage(StatusType.GET_ACCESS_DENIED,
						new KeyValue(key, "Cannot access data owned by another user"));
			}

			return new ServerMessage(value == null ? StatusType.GET_ERROR : StatusType.GET_SUCCESS,
					new KeyValue(key, value));
		}
		return new ServerMessage(StatusType.SERVER_NOT_RESPONSIBLE, new KeyValue(key, "Server is not responsible"),
				new KeyValue("meta", state.getMetadata().toMessageValue()));
	}

	/**
	 * Insert or update a pair in the database.
	 * 
	 * @param key
	 *            the key of the new pair
	 * @param value
	 *            the value of the new pair
	 * @return a KVMessage containing the new key and value if the pair is inserted
	 *         successfully, other return a KVMessage encoded the information why
	 *         the operation failed
	 */
	private KVMessage put(String owner, String key, String value) {
		return timedPut(owner, null, key, value);
	}

	/**
	 * This method insert a key value pair with timestamp to database
	 * 
	 * @param timeStamp
	 * @param key
	 * @param value
	 * @return a ServerMessage with put status and key value to client.
	 */
	private KVMessage timedPut(String owner, String timeStamp, String key, String value) {
		if (isLocked()) {
			return new ServerMessage(StatusType.SERVER_WRITE_LOCK,
					new KeyValue(key, "Cannot write while server is lock"));
		}

		if (state.getDb().get(key) != null && state.getDb().get(key).owner != owner) {
			return new ServerMessage(StatusType.UPDATE_ACCESS_DENIED,
					new KeyValue(key, "Cannot update data owned by another user"));
		}

		if (key.length() > MAX_KEY_LENGTH) {
			return new ServerMessage(StatusType.FAIL,
					new KeyValue("", String.format("Key cannot be longer than %d bytes, the sent key's length was %d",
							MAX_KEY_LENGTH, key.length())));
		}

		if (value.length() > MAX_VALUE_LENGTH) {
			return new ServerMessage(StatusType.FAIL,
					new KeyValue("",
							String.format("Value cannot be longer than %d bytes, the sent value's length was %d",
									MAX_VALUE_LENGTH, value.length())));
		}

		LocalDateTime delTime = timeStamp == null ? null : LocalDateTime.parse(timeStamp);
		StatusType resultStatus = null;
		if (delTime == null) {
			resultStatus = state.getDb().put(key, new KVData(value, owner, delTime)) == null ? StatusType.PUT_SUCCESS
					: StatusType.PUT_UPDATE;
		} else {
			resultStatus = state.getDb().put(key, new KVData(value, owner, delTime)) == null
					? StatusType.TIMED_PUT_SUCCESS
					: StatusType.TIMED_PUT_UPDATE;
		}

		state.getCache().put(key, value, owner);
		return new ServerMessage(resultStatus, new KeyValue(key, value));

	}

	/**
	 * Delete the pair with the associated key from the database, if the pair does
	 * not exist in the database, the application treats it as a successful
	 * procedure.
	 * 
	 * @param key
	 *            the key of the pair that needs to be deleted
	 * @return the KVMessage containing the key and value of the removed pair, value
	 *         is null if the pair does not exist in the database
	 */
	private KVMessage delete(String owner, String key) {

		if (isLocked()) {
			return new ServerMessage(StatusType.SERVER_WRITE_LOCK,
					new KeyValue(key, "Cannot write while server is lock"));
		}

		if (state.getDb().get(key) != null && state.getDb().get(key).owner != owner) {
			return new ServerMessage(StatusType.DELETE_ACCESS_DENIED,
					new KeyValue(key, "Cannot delete data owned by another user"));
		}

		KVData oldValue = state.getDb().remove(key);
		state.getCache().invalidate(key);
		return new ServerMessage(StatusType.DELETE_SUCCESS, new KeyValue(key, oldValue.value));
	}

	/**
	 * This method check whether password matchs username.
	 * 
	 * @param username
	 * @param password
	 * @return if match,return username to client. if not return LOGIN_ERROR and
	 *         notice to client.
	 * 
	 */
	private KVMessage login(String username, String password) {
		if (!state.getUserDb().authenticate(username, password)) {
			return new ServerMessage(StatusType.LOGIN_ERROR, new KeyValue(username, "Wrong username or password"));
		} else {
			return new ServerMessage(StatusType.LOGIN_SUCCESS, new KeyValue("", username));
		}

	}

	/**
	 * This method saves username and password to userDB, and call
	 * broadcastNewUSer() to broadcast this new user to all servers.
	 * 
	 * @param username
	 * @param password
	 * @return a ServerMessage containing SIGN_UP_SUCCESS status and username to
	 *         client
	 */
	private KVMessage signup(String username, String password) {
		if (!state.getUserDb().exists(username)) {
			try {
				state.getUserDb().put(username, password);
				broadcastNewUser(username, password);
				return new ServerMessage(StatusType.SIGN_UP_SUCCESS, new KeyValue("", username));
			} catch (IOException e) {
				logger.warn(e);
				return new ServerMessage(StatusType.SIGN_UP_ERROR,
						new KeyValue("", "Failed to create new user due to server's error"));
			}
		} else {
			return new ServerMessage(StatusType.SIGN_UP_ERROR, new KeyValue(username, "User already exists"));
		}

	}

	/**
	 * This method broadcast new user message to all servers by broasing metaTable.
	 * 
	 * @param username
	 * @param password
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	private void broadcastNewUser(String username, String password) throws UnknownHostException, IOException {
		for (MDEntry entry : state.getMetadata()) {
			DataDistributor dataDistributor = new DataDistributor(entry);
			dataDistributor.broadcastNewUser(username, password);
		}
	}

	/**
	 * Checks if the server is locked for writing
	 * 
	 * @return Nothing
	 */
	private boolean isLocked() {
		return state.getServerStatus() == Status.WRITELOCKED;
	}

	/**
	 * Checks if the server is Active
	 * 
	 * @return Nothing
	 */
	private boolean readyForNormalClient() {
		return state.getServerStatus() != Status.UNINITIALIZED && state.getServerStatus() != Status.STOPPED;
	}

	/**
	 * Checks if the key is in the range of the server
	 * 
	 * @return Nothing
	 */
	private boolean isResponsible(byte[] hash) {
		return Hash.in(hash, state.getMetadata().getPredecessor(state.getServerMeta()).hashIndex,
				state.getServerMeta().hashIndex);
	}

	/**
	 * This method estimates whether the server is respondible for the data by
	 * hashing the key
	 * 
	 * @param hash
	 * @return boolean true or false depending upon whether it is in the range.
	 */
	private boolean isResponsibleforGet(byte[] hash) {
		MDEntry iterEntry = state.getServerMeta();
		for (int i = 0; i < 3; i++) {
			iterEntry = state.getMetadata().getPredecessor(iterEntry);
		}
		return Hash.in(hash, state.getMetadata().getPredecessor(state.getServerMeta()).hashIndex,
				state.getServerMeta().hashIndex);
	}

}
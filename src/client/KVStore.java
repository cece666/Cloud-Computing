package client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import common.messages.ClientMessage;
import common.messages.KVMessage;
import common.messages.KeyValue;
import common.messages.StatusType;
import common.metadata.MDEntry;
import common.metadata.MDTable;
import common.util.MarshallUtils;

/**
 * This class interact with server.
 * 
 * @author Jiaxi Zhao
 *
 */
public class KVStore {
	public final String address;
	public final int port;
	private MDTable mdTable = null;
	private ConnectionManager connectionManager;
	private Logger logger = LogManager.getRootLogger();
	private String username = null;

	/**
	 * Initialize KVStore with address and port of KVServer
	 *
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		this.connectionManager = new ConnectionManager();
	}

	/**
	 * build connection to the server.
	 * 
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public KVMessage connect() throws UnknownHostException, IOException {
		Socket socket = getDefaultConnection();
		return processResponse(MarshallUtils.readFromServer(socket));
	}

	/**
	 * disconnect with socket
	 * 
	 * @throws IOException
	 */
	public void disconnect() throws IOException {
		connectionManager.close();
	}

	/**
	 * put key and value to the server.
	 * 
	 * @param key
	 * @param value
	 * @return rawMessage
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public KVMessage put(String key, String value) throws IllegalArgumentException, IOException {
		return authenticatedCommunicate(StatusType.PUT, new KeyValue(key, value));
	}

	/**
	 * This method put key, value and valuable time to the server.
	 * 
	 * @param timelapse
	 * @param key
	 * @param value
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public KVMessage timedPut(long timelapse, String key, String value) throws IllegalArgumentException, IOException {
		LocalDateTime dueTime = LocalDateTime.now().plusSeconds(timelapse);
		return authenticatedCommunicate(dueTime, StatusType.TIMED_PUT, new KeyValue(key, value));

	}

	/**
	 * This method let signed-up users log in by inputing username and password
	 * 
	 * @param username
	 * @param password
	 * @return
	 * @throws IOException
	 */
	public KVMessage login(String username, String password) throws IOException {
		return communicate(new ClientMessage(StatusType.LOGIN, new KeyValue(username, password)));
	}

	/**
	 * This method let users create their own account in the system by inputing
	 * username and password
	 * 
	 * @param username
	 * @param password
	 * @return
	 * @throws IOException
	 */
	public KVMessage signUp(String username, String password) throws IOException {
		return communicate(new ClientMessage(StatusType.SIGN_UP, new KeyValue(username, password)));
	}

	/**
	 * delete value by key
	 * 
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public KVMessage delete(String key) throws IOException {
		return authenticatedCommunicate(StatusType.DELETE, new KeyValue(key, null));

	}

	/**
	 * get KVMessage from the server by inputing key.
	 * 
	 * @param key
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public KVMessage get(String key) throws IllegalArgumentException, IOException {
		return authenticatedCommunicate(StatusType.GET, new KeyValue(key, null));
	}

	private KVMessage authenticatedCommunicate(StatusType status, KeyValue kv) throws IOException {
		return authenticatedCommunicate(null, status, kv);
	}

	private KVMessage authenticatedCommunicate(LocalDateTime dueTime, StatusType status, KeyValue kv)
			throws IOException {
		return communicate(new ClientMessage(username, dueTime, status, kv));
	}

	/**
	 * build communication by input KVMessage
	 * 
	 * @param message
	 * @return
	 * @throws IOException
	 */
	private KVMessage communicate(KVMessage message) throws IOException {
		return communicate(message, null);
	}

	/**
	 * build communication to server and receive KVMessage as a result
	 * 
	 * @param message
	 * @param socket
	 * @return
	 * @throws IOException
	 */
	private KVMessage communicate(KVMessage message, Socket socket) throws IOException {
		socket = socket != null ? socket : getConnection(message.getKey(), this::flushInfoMessage);
		MarshallUtils.writeToServer(message, socket);
		KVMessage receiveMessage = MarshallUtils.readFromServer(socket);
		return processResponse(receiveMessage);
	}

	/**
	 * process response according to KVMessage Status
	 * 
	 * @param message
	 * @return
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	private KVMessage processResponse(KVMessage message) throws IOException, UnknownHostException {
		if (message == null)
			return message;
		switch (message.getStatus()) {
		case INFO:
		case SERVER_NOT_RESPONSIBLE:
			this.mdTable = MDTable.fromMessageValue(message.getValue(1));
			break;
		case SERVER_STOPPED:
			disconnect();
			break;
		case LOGIN_SUCCESS:
		case SIGN_UP_SUCCESS:
			username = message.getValue();
		default:
			break;
		}

		return message;
	}

	@SuppressWarnings("unused")
	private Socket getConnection(String key) throws IOException {
		return getConnection(key, null);
	}

	/**
	 * search the metaTable for key's corresponding server, and connected to the
	 * server.
	 * 
	 * @param key
	 * @param newConnectionHandler
	 * @return
	 * @throws IOException
	 */
	private Socket getConnection(String key, Consumer<Socket> newConnectionHandler) throws IOException {
		MDEntry entry = mdTable.routeMessage(key);
		return connectionManager.getConnection(entry.addressPort, newConnectionHandler, this::identify);
	}

	private Socket getDefaultConnection() throws IOException {
		return getDefaultConnection(null);
	}

	/**
	 * get to the default server when doen't have metaTable.
	 * 
	 * @param newConnectionHandler
	 * @return
	 * @throws IOException
	 */
	private Socket getDefaultConnection(Consumer<Socket> newConnectionHandler) throws IOException {
		return connectionManager.getConnection(address, port, newConnectionHandler, this::identify);
	}

	/**
	 * Estimate whether there has already been a socket connection
	 * 
	 * @param socket
	 */
	private void identify(Socket socket) {
		try {
			MarshallUtils.writeToServer(new ClientMessage(StatusType.IDENTIFY, new KeyValue("", null)), socket);
		} catch (IllegalArgumentException | IOException e) {
			logger.warn("An error occured while identifying", e);
		}
	}

	/**
	 * flush the socket stream
	 * 
	 * @param socket
	 */
	private void flushInfoMessage(Socket socket) {
		try {
			MarshallUtils.readFromServer(socket);
		} catch (IOException e) {
			logger.warn("An error occured while flushing the stream", e);
		}
	}
}
package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import app_kvDatabase.KVData;
import cache.FIFOCache;
import cache.LFUCache;
import cache.LRUCache;
import cache.ServerCache;
import common.messages.KVMessage;
import common.messages.KeyValue;
import common.messages.ServerMessage;
import common.messages.StatusType;
import common.metadata.MDEntry;
import common.metadata.MDTable;
import common.util.MarshallUtils;
import failureDetector.FailureDetector;

/**
 * This class communicates with the ECS client after parsing its messages
 * 
 * @Author Aleena Yunus
 */
public class ECSHandler extends BaseHandler {
	private static Logger logger = LogManager.getLogger("kvServer");
	private final ServerState state;
	private final Runnable shutDown;

	public ECSHandler(Socket socket, ServerState state, Runnable shutDown) {
		super(socket, Executors.newCachedThreadPool(), logger);
		this.state = state;
		this.shutDown = shutDown;
	}

	/**
	 * Call the appropriate function depending on the message
	 * 
	 * @param message
	 *            the encoded KVMessage
	 * @return a callable that calls the appropriate function
	 */
	public KVMessage directMessage(KVMessage message) {
		switch (message.getStatus()) {
		case INIT:
			state.setServerMeta(MDEntry.fromValueString(message.getValue(0)));
			String meta = message.getValue(1);
			int cacheSize = Integer.parseInt(message.getValue(2));
			String displacementStrategy = message.getValue(3);
			return initKVServer(meta, cacheSize, displacementStrategy);
		case START:
			return start();
		case STOP:
			return stop();
		case SHUTDOWN:
			return shutDown();
		case LOCK_WRITE:
			return lockWrite();
		case UNLOCK_WRITE:
			return unLockWrite();
		case MOVE_DATA:
			return moveData(message.getValue(0), message.getValue(1));
		case UPDATE:
			return update(message.getValue(0));
		case IDENTIFY:
			return new ServerMessage(StatusType.INFO, new KeyValue("", "Already identify"));
		default:
			logger.error("Message's action is not in the allowed methods");
			throw new IllegalArgumentException("Message's action is not in the allowed methods");

		}
	}

	/**
	 * Initializes the server with metadata and cache
	 * 
	 * @param meta
	 *            metadata containing data about the servers
	 * @param cacheSize
	 *            size of cache
	 * @param displacementStrategy
	 *            algorithm for cache
	 * @return KVMessage a message to notify the ecs
	 */
	private KVMessage initKVServer(String meta, int cacheSize, String displacementStrategy) {
		if (state.getServerStatus() == KVServer.Status.UNINITIALIZED) {
			initializeCache(cacheSize, displacementStrategy);
			state.setMetadata(MDTable.fromMessageValue(meta));
			state.setServerStatus(KVServer.Status.STOPPED);
			logger.info("Server Initialized.");
			startFailureDetection();
			startReplication();
			startDestructor();
			return new ServerMessage(StatusType.DONE, new KeyValue("", "Server Initialized."));
		} else {
			logger.error("Server already initialized");
			return new ServerMessage(StatusType.FAIL, new KeyValue("", "Server already initialized."));
		}
	}

	/**
	 * Starts the server
	 * 
	 * @return KVMessage a message to notify the ecs
	 */
	private KVMessage start() {
		if (state.getServerStatus() == KVServer.Status.UNINITIALIZED
				|| state.getServerStatus() == KVServer.Status.STOPPED) {
			state.setServerStatus(KVServer.Status.ACTIVE);
			logger.info("Server Started.");
			return new ServerMessage(StatusType.DONE, new KeyValue("", "Server Started."));
		} else {
			logger.error("Server already active or in write lock");
			return new ServerMessage(StatusType.FAIL, new KeyValue("", "Server already active or in write lock."));
		}
	}

	/**
	 * Stops the server
	 * 
	 * @return KVMessage a message to notify the ecs
	 */
	private KVMessage stop() {
		if (state.getServerStatus() == KVServer.Status.ACTIVE) {
			state.setServerStatus(KVServer.Status.STOPPED);
			logger.info("Server Stopped.");
			return new ServerMessage(StatusType.DONE, new KeyValue("", "Server Stopped."));
		} else {
			logger.error("Server initialized, already stopped or in write lock");
			return new ServerMessage(StatusType.FAIL,
					new KeyValue("", "Server uninitialized, already stopped or in write lock."));
		}
	}

	/**
	 * Shuts the server down
	 * 
	 * @return KVMessage a message to notify the ecs
	 */
	private KVMessage shutDown() {
		logger.info("Server shutting down");
		shutDown.run();
		return new ServerMessage(StatusType.DONE, new KeyValue("", "Server Shutting down..."));
	}

	/**
	 * Puts the server on lock write
	 * 
	 * @return KVMessage a message to notify the ecs
	 */
	private KVMessage lockWrite() {
		if (state.getServerStatus() == KVServer.Status.ACTIVE) {
			state.setServerStatus(KVServer.Status.WRITELOCKED);
			logger.info("Server locked for writing.");
			return new ServerMessage(StatusType.DONE, new KeyValue("", "Server locked for writing."));
		} else {
			logger.error("Server stopped or unitialized");
			return new ServerMessage(StatusType.FAIL, new KeyValue("", "Server stopped or uninitialized."));
		}
	}

	/**
	 * Unlocks the server to enable writing
	 *
	 * @return KVMessage a message to notify the ecs
	 */
	private KVMessage unLockWrite() {
		if (state.getServerStatus() == KVServer.Status.WRITELOCKED) {
			state.setServerStatus(KVServer.Status.ACTIVE);
			logger.info("Server unlocked for writing.");
			deleteOutOfRangeData();

			return new ServerMessage(StatusType.DONE, new KeyValue("", "Server unlocked for writing."));
		} else {
			logger.error("Server has to be locked to unlock");

			return new ServerMessage(StatusType.FAIL, new KeyValue("", "Server has to be locked to unlock."));
		}
	}

	/**
	 * Moves the data from one server to another within a range
	 * 
	 * @param from
	 *            this index to start moving from
	 * @param to
	 *            the index to move to and also the upper bound for moving the data
	 * @param newServer
	 *            server to send the data to
	 * @return KVMessage a message to notify the ecs
	 */
	private KVMessage moveData(String from, String to) {
		try {
			MDEntry source = MDEntry.fromConfigString(from);
			MDEntry destination = MDEntry.fromConfigString(to);

			HashMap<String, KVData> dataToBeMoved = state.getDb().dataInRange(source.hashIndex, destination.hashIndex);

			DataDistributor dataDistributor = new DataDistributor(destination);

			ArrayList<KeyValue> kvs = dataToBeMoved.entrySet().stream()
					.flatMap(i -> Stream.of(new KeyValue(i.getKey(), i.getValue().value),
							new KeyValue("owner", i.getValue().owner),
							new KeyValue("delTime", i.getValue().delTime.toString())))
					.collect(Collectors.toCollection(ArrayList::new));
			dataDistributor.moveData(kvs);

		} catch (IOException e) {
			logger.error(e);
			return new ServerMessage(StatusType.FAIL, new KeyValue("", "Data move failed."));
		}
		logger.info("Data moved.");
		return new ServerMessage(StatusType.DONE, new KeyValue("", "Data moved."));
	}

	/**
	 * Updates the metadata and removes out of range elements
	 * 
	 * @param meta
	 *            updated metadata containing data about the servers
	 * @return KVMessage a message to notify the ecs
	 */
	private KVMessage update(String meta) {
		if (state.getServerStatus() == KVServer.Status.UNINITIALIZED) {
			logger.error("Server unitialized");

			return new ServerMessage(StatusType.FAIL, new KeyValue("", "Server uninitialized."));
		}

		state.setMetadata(MDTable.fromMessageValue(meta));
		logger.info("Metadata updated.");
		return new ServerMessage(StatusType.DONE, new KeyValue("", "Metadata updated."));
	}

	/**
	 * This method initializes the cache object depending upon the strategy selected
	 * at the start of the server.
	 */
	private void initializeCache(int cacheSize, String strategy) {
		if (strategy.toUpperCase().equals(ServerCache.FIFO)) {
			state.setCache(new FIFOCache(cacheSize));
		} else if (strategy.toUpperCase().equals(ServerCache.LRU)) {
			state.setCache(new LRUCache(cacheSize));
		} else {
			state.setCache(new LFUCache(cacheSize));
		}
	}

	/**
	 * This method starts failure detection routine
	 */
	private void startFailureDetection() {
		FailureDetector fd = new FailureDetector(state);
		service.execute(fd);
		logger.info("Failure detection routine started...");
	}

	/**
	 * This method starts replication thread
	 */
	private void startReplication() {
		Replication replication = new Replication(state);
		service.execute(replication);
		logger.info("Replication started...");
	}

	private void startDestructor() {
		Destroyer destroyerTask = new Destroyer(state);
		service.execute(destroyerTask);
		logger.info("Task for deleting temp data started...");
	}

	/**
	 * This method deletes out of range data
	 */
	private void deleteOutOfRangeData() {
		byte[] start = state.getMetadata().getPredecessor(state.getServerMeta()).hashIndex;
		byte[] end = state.getServerMeta().hashIndex;
		HashMap<String, KVData> dataToBeDeleted = state.getDb().dataOutOfRange(start, end);
		for (Entry<String, KVData> entry : dataToBeDeleted.entrySet()) {
			state.getDb().remove(entry.getKey());
		}
	}

	/*
	 * This method run before proceeding ECS commands to send INFO-status message to
	 * server (non-Javadoc)
	 * 
	 * @see app_kvServer.BaseHandler#preRun()
	 */
	@Override
	protected void preRun() throws IOException {
		KVMessage message = new ServerMessage(StatusType.INFO, new KeyValue("", "Finish starting server"));
		MarshallUtils.writeToServer(message, socket);
	}
}

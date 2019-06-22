package app_kvEcs;

import static common.messages.StatusType.IDENTIFY;
import static common.messages.StatusType.LOCK_WRITE;
import static common.messages.StatusType.SHUTDOWN;
import static common.messages.StatusType.START;
import static common.messages.StatusType.STOP;
import static common.messages.StatusType.UNLOCK_WRITE;
import static common.messages.StatusType.UPDATE;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Stream.of;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import client.ConnectionManager;
import common.messages.ECSMessage;
import common.messages.KVMessage;
import common.messages.KeyValue;
import common.messages.StatusType;
import common.metadata.AddressPort;
import common.metadata.MDEntry;
import common.metadata.MDTable;
import common.util.MarshallUtils;

/**
 * This class build the connection between servers and ECS, and pass commands
 * between ECS and servers.
 * 
 * @author Jiaxi Zhao
 *
 */
public class ECSCommunication {
	private static Logger logger = LogManager.getLogger("kvECS");
	private static final ServerConfig DEFAULT_CONFIG = new ServerConfig(1, "FIFO");

	private static final String CONFIG_PATH = "servers.config";

	private static final KVMessage START_COMMAND = new ECSMessage(START);
	private static final KVMessage STOP_COMMAND = new ECSMessage(STOP);
	private static final KVMessage SHUTDOWN_COMMAND = new ECSMessage(SHUTDOWN);
	private static final KVMessage LOCK_WRITE_COMMAND = new ECSMessage(LOCK_WRITE);
	private static final KVMessage UNLOCK_WRITE_COMMAND = new ECSMessage(UNLOCK_WRITE);
	private static final KVMessage IDENTIFY_COMMAND = new ECSMessage(IDENTIFY);

	private static final int TRY_TIMES = 100;
	private static final int WAIT_TIME = 500;

	private final HashMap<MDEntry, ServerConfig> configs = new HashMap<>();
	private final ConnectionManager connectionManager = new ConnectionManager();
	private final Recovery recovery;

	private MDTable metaTable;
	private MDTable uninitializedMetaTable;

	private boolean canInit = true;

	public ECSCommunication() throws IOException {
		this.metaTable = new MDTable(new ArrayList<>());
		this.uninitializedMetaTable = MDTable.fromConfigFile(new File(CONFIG_PATH));
		this.recovery = new Recovery(this::recover);
	}

	/**
	 * Init servers
	 * 
	 * @param numberOfNodes
	 * @param cacheSize
	 * @param displacementStrategy
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void initService(int numberOfNodes, int cacheSize, String displacementStrategy)
			throws IOException, InterruptedException {
		synchronized (metaTable) {
			long start = System.nanoTime();
			if (!canInit) {
				System.out.println("ECS already initialized some servers");
				return;
			}

			metaTable = uninitializedMetaTable.popNode(numberOfNodes);

			for (MDEntry entry : metaTable) {
				startServer(entry.addressPort.address, entry.addressPort.port);
				System.out.printf("Start server at %s:%d\n", entry.addressPort.address, entry.addressPort.port);
			}

			tryBroadcast(IDENTIFY_COMMAND, TRY_TIMES * numberOfNodes / 5, WAIT_TIME);
			final ServerConfig config = new ServerConfig(cacheSize, displacementStrategy);
			for (MDEntry entry : metaTable) {
				configs.put(entry, config);
			}

			Function<MDEntry, KVMessage> initMapper = (entry) -> init(entry, config);
			broadcast(initMapper);

			canInit = false;
			long timeTook = (System.nanoTime() - start) / 1000000;
			System.out.printf("Took %dms for initializing %d node(s)\n", timeTook, numberOfNodes);

		}
	}

	/**
	 * Start servers
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public void start() throws IllegalArgumentException, IOException {
		broadcast(START_COMMAND);
	}

	/**
	 * Stop Servers
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public void stop() throws IllegalArgumentException, IOException {
		broadcast(STOP_COMMAND);
	}

	/**
	 * Shut down servers
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public void shutDown() throws IllegalArgumentException, IOException {

		synchronized (metaTable) {
			for (MDEntry entry : metaTable) {
				configs.remove(entry);
			}
			System.out.println("Shutting down all initialized servers");
			broadcasShutDown();
			canInit = true;
		}
	}

	/**
	 * add one servers
	 * 
	 * @param cacheSize
	 * @param displacementStrategy
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void addNode(ServerConfig config) throws IOException, InterruptedException {
		synchronized (metaTable) {
			MDEntry newEntry = uninitializedMetaTable.popNode()[1];
			metaTable.addEntry(newEntry);
			MDEntry successor = metaTable.getSuccessor(newEntry);
			MDEntry predecessor = metaTable.getPredecessor(newEntry);

			configs.put(newEntry, config);

			startServer(newEntry.addressPort.address.toString(), newEntry.addressPort.port);

			trySend(IDENTIFY_COMMAND, newEntry, TRY_TIMES, WAIT_TIME);

			ECSMessage initMessage = init(newEntry, config);
			send(initMessage, newEntry);
			getResponse(newEntry);

			sendAndGet(LOCK_WRITE_COMMAND, successor);

			ECSMessage moveMessage = moveData(predecessor, newEntry);
			sendAndGet(moveMessage, successor);

			sendAndGet(UNLOCK_WRITE_COMMAND, successor);

			broadcastMetatable();
			System.out.printf("Finish add node at %s:%d\n", newEntry.addressPort.address, newEntry.addressPort.port);
		}
	}

	/**
	 * remove one server
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public void removeNode() throws IllegalArgumentException, IOException {
		synchronized (metaTable) {
			MDEntry[] popData = metaTable.popNode();

			MDEntry previousPredecessor = popData[0];
			MDEntry deleteEntry = popData[1];
			MDEntry previousSuccessor = popData[2];

			configs.remove(deleteEntry);

			uninitializedMetaTable.addEntry(deleteEntry);

			sendAndGet(LOCK_WRITE_COMMAND, deleteEntry);

			KVMessage moveMessage = moveData(previousPredecessor, previousSuccessor);
			sendAndGet(moveMessage, deleteEntry);

			sendAndGet(UNLOCK_WRITE_COMMAND, deleteEntry);

			send(SHUTDOWN_COMMAND, deleteEntry);
			removeConnection(deleteEntry.addressPort).close();

			broadcastMetatable();
		}
	}

	/**
	 * This method invokes recovery Thread.
	 * 
	 * @param client
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public void startRecovery() throws IllegalArgumentException, IOException {
		new Thread(recovery).start();
	}

	private void recover(Set<MDEntry> failNodes) {
		synchronized (metaTable) {
			ArrayList<MDEntry> entries = new ArrayList<>(failNodes);
			metaTable.removeEntries(entries);
			uninitializedMetaTable.addEntries(entries);

			for (MDEntry entry : entries) {
				ServerConfig config = configs.get(entry);
				if (config == null) {
					config = DEFAULT_CONFIG;
				}

				try {
					addNode(config);
				} catch (IOException | InterruptedException e) {
					logger.warn(e);
				}
			}
		}
	}

	/**
	 * start servers by sending running .sh file
	 * 
	 * @param address
	 * @param port
	 * @throws IOException
	 */
	private Process startServer(String address, int port) throws IOException {
		String recoverListenerAddress = recovery.getAddress().getHostAddress();
		String recoverListenerPort = Integer.toString(recovery.getPort());
		return new ProcessBuilder("./init_server.sh", address, Integer.toString(port), recoverListenerAddress,
				recoverListenerPort).start();
	}

	/**
	 * form string which combine predecessor and successor
	 * 
	 * @param from
	 * @param to
	 * @return
	 */
	private ArrayList<KeyValue> moveDataMessage(MDEntry from, MDEntry to) {
		return of(new KeyValue("from", from.valueString()), new KeyValue("to", to.valueString()))
				.collect(toCollection(ArrayList::new));
	}

	/**
	 * Combine node message to String
	 * 
	 * @param entry
	 * @param cacheSize
	 * @param displacementStrategy
	 * @return
	 */
	private ArrayList<KeyValue> newNodeMessage(MDEntry entry, int cacheSize, String displacementStrategy) {
		return of(new KeyValue("target", entry.valueString()), new KeyValue("meta", metaTable.toMessageValue()),
				new KeyValue("cacheSize", Integer.toString(cacheSize)),
				new KeyValue("displacementStratergy", displacementStrategy)).collect(toCollection(ArrayList::new));
	}

	/**
	 * broadcast metatable to all servers.
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public void broadcastMetatable() throws IllegalArgumentException, IOException {
		KVMessage metatable = new ECSMessage(UPDATE, new KeyValue("", metaTable.toMessageValue()));
		broadcast(metatable);
	}

	/**
	 * shutDown all servers and clear metaTable
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void broadcasShutDown() throws IllegalArgumentException, IOException {
		synchronized (metaTable) {
			uninitializedMetaTable.mergeTable(metaTable);
			for (MDEntry entry : metaTable) {
				send(SHUTDOWN_COMMAND, entry);
				removeConnection(entry.addressPort).close();
			}
			metaTable.clear();
		}
	}

	/**
	 * Broadcast KVMessage by mapper.
	 * 
	 * @param mapper
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void broadcast(Function<MDEntry, KVMessage> mapper) throws IllegalArgumentException, IOException {
		synchronized (metaTable) {
			for (MDEntry entry : metaTable) {
				KVMessage response = sendAndGet(mapper.apply(entry), entry);
				processResponse(response);
			}
		}
	}

	/**
	 * broadcast KVMessage
	 * 
	 * @param message
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void broadcast(KVMessage message) throws IllegalArgumentException, IOException {
		synchronized (metaTable) {
			for (MDEntry entry : metaTable) {
				KVMessage response = sendAndGet(message, entry);
				processResponse(response);
			}

		}
	}

	/**
	 * 
	 * @param message
	 * @param times
	 * @param sleepMs
	 * @throws IllegalArgumentException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void tryBroadcast(KVMessage message, int times, int sleepMs)
			throws IllegalArgumentException, IOException, InterruptedException {
		synchronized (metaTable) {
			for (MDEntry entry : metaTable) {
				trySend(message, entry, times, sleepMs);
				processResponse(getResponse(entry));
			}
		}
	}

	/**
	 * process received KVMessage
	 * 
	 * @param response
	 */
	private void processResponse(KVMessage response) {
		if (response == null) {
			System.out.println("Stream is closed");
			return;
		}
		if (response.getValue() != null) {
			System.out.println(response.getValue());
			logger.info(response.getValue());
		}
	}

	/**
	 * send KVMessage to entry's accroding server.
	 * 
	 * @param message
	 * @param entry
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void send(KVMessage message, MDEntry entry) throws IllegalArgumentException, IOException {
		MarshallUtils.writeToServer(message, getConnection(entry.addressPort));
	}

	/**
	 * receive get KVMessage from server.
	 * 
	 * @param entry
	 * @return
	 * @throws IOException
	 */
	private KVMessage getResponse(MDEntry entry) throws IOException {
		return MarshallUtils.readFromServer(getConnection(entry.addressPort));
	}

	/**
	 * send message to entry's corresponding server
	 * 
	 * @param message
	 * @param entry
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private KVMessage sendAndGet(KVMessage message, MDEntry entry) throws IllegalArgumentException, IOException {
		send(message, entry);
		return getResponse(entry);
	}

	/**
	 * get socket connection by addressPort
	 * 
	 * @param addressPort
	 * @return
	 * @throws IOException
	 */
	private Socket getConnection(AddressPort addressPort) throws IOException {
		return connectionManager.getConnection(addressPort);
	}

	/**
	 * remove Socket from connectionMap
	 * 
	 * @param addressPort
	 * @return
	 */
	private Socket removeConnection(AddressPort addressPort) {
		return connectionManager.remove(addressPort);
	}

	/**
	 * try send message to the server for several times.
	 * 
	 * @param message
	 * @param entry
	 * @param times
	 * @param sleepMs
	 * @throws IllegalArgumentException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void trySend(KVMessage message, MDEntry entry, int times, int sleepMs)
			throws IllegalArgumentException, IOException, InterruptedException {

		for (int i = 0; i < times - 1; i++) {
			try {
				send(message, entry);
				return;
			} catch (IOException e) {
				logger.warn(e);
				TimeUnit.MILLISECONDS.sleep(sleepMs);
			}
		}

		send(message, entry);
	}

	private ECSMessage init(MDEntry newEntry, ServerConfig config) {
		return new ECSMessage(StatusType.INIT, newNodeMessage(newEntry, config.cacheSize, config.displacementStrategy));
	}

	private ECSMessage moveData(MDEntry predecessor, MDEntry newEntry) {
		return new ECSMessage(StatusType.MOVE_DATA, moveDataMessage(predecessor, newEntry));
	}
}

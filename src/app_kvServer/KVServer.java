package app_kvServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import app_kvDatabase.KVDatabase;
import app_kvDatabase.UserDatabase;
import common.metadata.AddressPort;

/**
 * This class handle the connection to the server.
 * 
 * @author Uy Ha
 *
 */
public class KVServer {

	public enum Status {
		UNINITIALIZED, ACTIVE, STOPPED, WRITELOCKED;
	}

	private static final Logger logger = LogManager.getLogger("kvServer");
	private final int port;
	private final ExecutorService service;
	private final ServerState state;
	private ServerSocket serverSocket;
	private ArrayList<Socket> addedSocket = new ArrayList<>();

	/**
	 * set the log level
	 */
	private static final HashMap<String, Level> LEVEL_MAP = new HashMap<>();
	static {
		LEVEL_MAP.put("ALL", Level.ALL);
		LEVEL_MAP.put("TRACE", Level.TRACE);
		LEVEL_MAP.put("DEBUG", Level.DEBUG);
		LEVEL_MAP.put("INFO", Level.INFO);
		LEVEL_MAP.put("WARN", Level.WARN);
		LEVEL_MAP.put("ERROR", Level.ERROR);
		LEVEL_MAP.put("FATAL", Level.FATAL);
		LEVEL_MAP.put("OFF", Level.OFF);
	}

	/**
	 * Start KV Server at given port
	 *
	 * @param port given port for storage server to operate
	 * @throws IOException when the database does not exist and the application
	 *                     failed to create a new one
	 */
	public KVServer(int port, AddressPort addressPort) throws IOException {
		this.port = port;
		this.service = Executors.newCachedThreadPool();
		this.state = new ServerState(new KVDatabase(String.format("./db%d.kv", port)),
				new KVDatabase(String.format("./replica1_%d.kv", port)),
				new KVDatabase(String.format("./replica2_%d.kv", port)),
				new UserDatabase(String.format("./userDb_%d.kv", port)), addressPort);
	}

	/**
	 * Initializes and starts the server. Loops until the the server should be
	 * closed.
	 */
	public void run() {
		while (!serverSocket.isClosed()) {
			try {
				Socket client = serverSocket.accept();
				addedSocket.add(client);
				HandlerRouter router = new HandlerRouter(client, state, service, this::stopServer);
				service.execute(router);

				logger.info("Connected to " + client.getInetAddress().getHostName() + " on port " + client.getPort());
			} catch (IOException e) {
				logger.error("Error! " + "Unable to establish connection. \n", e);
			}
		}
	}

	/**
	 * Stops the server insofar that it won't listen at the given port any more.
	 */
	public void stopServer() {
		try {
			serverSocket.close();
			for (Socket socket : addedSocket) {
				socket.close();
			}
			logger.info("Server stopped.");
		} catch (IOException e) {
			logger.error("Error! " + "Unable to close socket on port: " + port, e);
		} finally {
			System.exit(0);
		}
	}

	/**
	 * Initialize the server
	 * 
	 * @throws IOException if the socket cannot be opened
	 */
	private void initializeServer() throws IOException {
		logger.info("Initialize server ...");
		serverSocket = new ServerSocket(port);
		logger.info("Server listening on port: " + serverSocket.getLocalPort());
	}

	/**
	 * Start the server, initialize the socket and cache
	 * 
	 * @param port     the port the server going to use for the socket
	 * @param size     the cache size
	 * @param strategy the strategy that the cache is going to use
	 */
	public static void startServer(int port, String escAddress, int ecsPort) {
		try {
			AddressPort addressPort = new AddressPort(escAddress, ecsPort);
			KVServer server = new KVServer(port, addressPort);
			server.initializeServer();
			server.run();
		} catch (IOException e) {
			logger.error(e);
		}
	}

	/**
	 * parse string array and disassemble the string to start a server.
	 * 
	 * @param args
	 */
	public static void parseArgsAndStartServer(String[] args) {
		int port = Integer.parseInt(args[0]);
		String ecsAddress = args[1];
		int ecsPort = Integer.parseInt(args[2]);
		startServer(port, ecsAddress, ecsPort);
		Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
			e.printStackTrace();
		});
	}

	public static void main(String[] args) {
		try {
			if (args.length < 3) {
				System.out.println("Expecting <Server Port> <ECS IP> <ECS Port> ");
			} else {
				parseArgsAndStartServer(args);

			}
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
		}
	}
}

package app_kvClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import client.ClientFormat;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.StatusType;

/**
 * This class parses the input from the user and takes action accordingly.
 * 
 * @author Jiaxi Zhao
 *
 */
public class KVClient {
	private static final String UNKNOWN_COMMAND = "Unknown command";
	private static final String NOT_CONNECTED = "Application has not connected to the server";
	private static final String CLOSED_CONNECTION = "Connection to server is close, please create a new connection";

	/**
	 * Different command patterns
	 */
	private static final Pattern connectPattern = Pattern.compile("connect\\s+([^\\s]+)\\s+(\\d+)");
	private static final Pattern disconnectPattern = Pattern.compile("disconnect");
	private static final Pattern putPattern = Pattern.compile("put\\s+([^\\s]+)\\s+([^\\s]+)");
	private static final Pattern timedPutPattern = Pattern.compile("timedput\\s+(\\d+)\\s+([^\\s]+)\\s+([^\\s]+)");
	private static final Pattern loginPattern = Pattern.compile("login");
	private static final Pattern signUpPattern = Pattern.compile("signup");
	private static final Pattern deletePattern = Pattern.compile("delete\\s+([^\\s]+)$");
	private static final Pattern getPattern = Pattern.compile("get\\s+([^\\s]+)");
	private static final Pattern logLevelPattern = Pattern.compile("logLevel\\s+(.*)");
	private static final Pattern helpPattern = Pattern.compile("help");
	private static final Pattern quitPattern = Pattern.compile("quit");

	/**
	 * Mapping of log levels
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

	private KVStore kvStore;
	private Logger logger = LogManager.getRootLogger();

	/**
	 * This method recognize the command entered by the user, matches the command to
	 * the predefined command patterns and calls the method corresponding to the
	 * command entered or help() if the command is unknown.
	 * 
	 * @param command
	 *            the command entered by the user
	 * @throws IOException
	 */
	public void processCommand(String command) {
		logger.info(command);
		Matcher matcher;
		try {
			if ((matcher = connectPattern.matcher(command)).find()) {
				String address = matcher.group(1);
				String port = matcher.group(2);
				connect(address, port);
			} else if ((matcher = disconnectPattern.matcher(command)).find())
				disconnect();
			else if ((matcher = timedPutPattern.matcher(command)).find())
				timedPut(matcher.group(1), matcher.group(2), matcher.group(3));
			else if ((matcher = putPattern.matcher(command)).find())
				put(matcher.group(1), matcher.group(2));
			else if ((matcher = loginPattern.matcher(command)).find())
				login();
			else if ((matcher = signUpPattern.matcher(command)).find())
				signUp();
			else if ((matcher = deletePattern.matcher(command)).find())
				delete(matcher.group(1));
			else if ((matcher = getPattern.matcher(command)).find())
				get(matcher.group(1));
			else if ((matcher = logLevelPattern.matcher(command)).find())
				logLevel(matcher.group(1));
			else if ((matcher = helpPattern.matcher(command)).find())
				help();
			else if ((matcher = quitPattern.matcher(command)).find())
				quit();
			else {
				System.out.println(UNKNOWN_COMMAND);
				help();
			}
		} catch (IOException | IllegalArgumentException e) {
			System.out.println(e.getMessage());
			logger.error(e);
		}
	}

	/**
	 * This method calls the connect method in KVStore class and does required
	 * exception handling.
	 * 
	 * @param host
	 *            the address of the server
	 * @param portString
	 *            the port at which server is listening
	 * @throws IOException
	 * @exception NumberFormatException
	 */
	private void connect(String host, String portString) throws IOException {
		try {
			int port = Integer.parseInt(portString);
			connect(host, port);
		} catch (NumberFormatException e) {
			kvStore = null;
			logger.error(e);
			printMessage(e.getMessage());
		} catch (IOException e) {
			kvStore = null;
			throw e;
		}
	}

	/**
	 * This method calls the connect method in KVStore class and does required
	 * exception handling.
	 * 
	 * @param host
	 *            the address of the server
	 * @param port
	 *            the port at which server is listening
	 * @throws IOException
	 */
	private void connect(String host, int port) throws IOException {
		if (kvStore == null) {
			kvStore = new KVStore(host, port);
			try {
				KVMessage response = kvStore.connect();
				printMessage(ClientFormat.formatKVMessage(response));
				if (brokenMessage(response)) {
					return;
				}
			} catch (Exception e) {
				kvStore = null;
				throw e;
			}
		} else {
			System.out.println("Already connected to server, please disconnect first");
		}
	}

	/**
	 * This method close the connection to the server.
	 * 
	 * @exception IOException
	 */
	private void disconnect() throws IOException {
		if (kvStore != null) {
			kvStore.disconnect();
			System.out.printf("Disconnecting from %s:%d\n", kvStore.address, kvStore.port);
			kvStore = null;
		} else {
			System.out.println("Application has not connected to server, nothing to do");
		}
	}

	/**
	 * This method calls for put method in KVStore and print out the responseStatus
	 * from the server.
	 * 
	 * @param key
	 *            the key of the pair
	 * @param value
	 *            the value of the pair
	 * @exception IOException
	 * @exception IllegalArgumentException
	 */
	private void put(String key, String value) throws IllegalArgumentException, IOException {
		if (kvStore != null) {
			KVMessage response = kvStore.put(key, value);
			printMessage(ClientFormat.formatKVMessage(response));
			if (brokenMessage(response)) {
				return;
			}
		} else {
			System.out.println(NOT_CONNECTED);
		}
	}

	/**
	 * This method calls for timedPut method in KVStore and print out the
	 * responseStatus from the server.
	 * 
	 * @param timelapse
	 * @param key
	 * @param value
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void timedPut(String timelapse, String key, String value) throws IllegalArgumentException, IOException {
		if (kvStore != null) {
			KVMessage response = kvStore.timedPut(Long.parseLong(timelapse), key, value);
			printMessage(ClientFormat.formatKVMessage(response));
			if (brokenMessage(response)) {
				return;
			}
		} else {
			System.out.println(NOT_CONNECTED);
		}
	}

	/**
	 * This method calls for get method in KVStore and print out the responseStatus
	 * from the server.
	 * 
	 * @param key
	 *            the key of the pair
	 * @exception IOException,IllegalArgumentException
	 */
	private void get(String key) throws IllegalArgumentException, IOException {
		if (kvStore != null) {
			KVMessage response = kvStore.get(key);
			printMessage(ClientFormat.formatKVMessage(response));
			if (brokenMessage(response)) {
				return;
			}
		} else {
			System.out.println(NOT_CONNECTED);
		}
	}

	/**
	 * This method calls for delete method in KVStore and print out the
	 * responseStatus from the server.
	 * 
	 * @param key
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void delete(String key) throws IllegalArgumentException, IOException {
		if (kvStore != null) {
			KVMessage response = kvStore.delete(key);
			printMessage(ClientFormat.formatKVMessage(response));
			if (brokenMessage(response)) {
				return;
			}
		} else {
			System.out.println(NOT_CONNECTED);
		}
	}

	/**
	 * This method calls for log in method in KVStore
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void login() throws IllegalArgumentException, IOException {
		Scanner scanner = new Scanner(System.in);
		System.out.print("Username>");
		String username = scanner.nextLine();
		System.out.print("Password>");
		String password = scanner.nextLine();
		// String password = new String(System.console().readPassword("Password>"));
		if (kvStore != null) {
			KVMessage response = kvStore.login(username, password);
			printMessage(ClientFormat.formatKVMessage(response));
			if (brokenMessage(response)) {
				return;
			}
		} else {
			System.out.println(NOT_CONNECTED);
		}
	}

	/**
	 * This method calls for signUp method in KVStore
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void signUp() throws IllegalArgumentException, IOException {
		Scanner scanner = new Scanner(System.in);
		System.out.print("Username>");
		String username = scanner.nextLine();
		System.out.print("Password>");
		String password = scanner.nextLine();
		System.out.print("Confirm Password>");
		String password2 = scanner.nextLine();
		// String password = new String(System.console().readPassword("Password>"));
		// String password2 = new String(System.console().readPassword("Confirm
		// Password>"));
		if (!password.equals(password2)) {
			System.out.print("The entered passwords don't match");
			return;
		}

		if (kvStore != null) {
			KVMessage response = kvStore.signUp(username, password);
			printMessage(ClientFormat.formatKVMessage(response));
			if (brokenMessage(response)) {
				return;
			}
		} else {
			System.out.println(NOT_CONNECTED);

		}
	}

	/**
	 * Estimate whether connection is built successful or not.
	 * 
	 * @param message
	 * @return
	 */
	private boolean brokenMessage(KVMessage message) {
		// TODO: Check if needs update for login and signup
		if (message == null) {
			kvStore = null;
			printMessage(CLOSED_CONNECTION);
			return true;
		}

		if (message.getStatus() == StatusType.SERVER_STOPPED) {
			kvStore = null;
			return true;
		}

		return false;
	}

	/**
	 * This method validates the entered log level and change the log level.
	 * 
	 * @param level
	 *            the target level for the logger to change to
	 * @throws IllegalArgumentException
	 */
	private void logLevel(Level level) throws IllegalArgumentException {
		if (level != null) {
			Configurator.setLevel(logger.getName(), level);
			System.out.printf("Log level is changed to %s\n", logger.getLevel());
		} else {
			throw new IllegalArgumentException("Target log level cannot be null");
		}
	}

	/**
	 * This method validates the entered log level and change the log level.
	 * 
	 * @param levelString
	 * @throws IllegalArgumentException
	 */
	private void logLevel(String levelString) throws IllegalArgumentException {
		Level level = LEVEL_MAP.get(levelString.toUpperCase());
		if (level != null)
			logLevel(level);
		else
			throw new IllegalArgumentException(String.format("%s is not a valid level", levelString));
	}

	/**
	 * This method prints out the help manual
	 */
	public void help() {
		final String helpString = "Commands:\n"
				+ "connect <addr> <port> \t Connects you to the server at the given ip & port"
				+ "\nlogin \\t\\t login in to the storage service with your username and password"
				+ "\nsignUp \\t\\t Register with the storage service"
				+ "\ndisconnect \t\t Disconnects from the connected server"
				+ "\nput <key><value> \t\t Inserts a key-value pair into the storage server data structures.\r\n"
				+ "\ntimedput <timedInSeconds><key><value> \t\t Inserts a key-value pair into the storage server data structures for a specific amount of time.\r\n"
				+ "Updates (overwrites) the current value with the given value if the server already contains the specified key.\r\n"
				+ "\ndelete <key> Deletes the entry for the given key."
				+ "\nget<key>\t\tRetrieves the value for the given key from the storage server."
				+ "\nlogLevel <level> \t Specifies the logging level"
				+ "\nhelp \t\t\t Shows you this page but you probably already know that"
				+ "\nquit \t\t\t Disconnects from the server and exits the program\n";
		System.out.println(helpString);
	}

	/**
	 * This method disconnects from the server and exits the running program.
	 */
	public void quit() throws IOException {
		try {
			disconnect();
		} finally {
			logger.info("Application is exiting");
			System.out.println("Application is exiting");
			System.exit(1);
		}
	}

	public static void printMessage(String message) {
		System.out.printf("Client>%s\n", message);
	}

	/**
	 * This main method create a KVClient object and execute the methods in KVClient
	 * by receiving the commands from user by scanner.
	 */
	public static void main(String[] args) {
		if (args.length > 0) {
			try {
				perfomanceMeasure(Integer.parseInt(args[0]));
			} catch (NumberFormatException | IOException e) {
				System.out.println(e.getMessage());
			}
			return;
		}
		KVClient client = new KVClient();
		try (Scanner scanner = new Scanner(System.in)) {
			while (true) {
				System.out.print("Client>");
				client.processCommand(scanner.nextLine());
			}
		} catch (NoSuchElementException e) {
			System.out.println();
			client.processCommand("quit");
		}
	}

	/**
	 * This method measures the performance of the system
	 * 
	 * @param nClients
	 * @throws IOException
	 */
	private static void perfomanceMeasure(int nClients) throws IOException {
		Random random = new Random();
		ArrayList<KVClient> clients = new ArrayList<>();
		for (int i = 0; i < nClients; i++) {
			clients.add(new KVClient());
		}
		for (KVClient c : clients) {
			c.connect("localhost", 50000);
		}

		long start = System.nanoTime();
		for (KVClient c : clients) {
			long startEach = System.nanoTime();
			for (int i = 0; i < 100; i++) {
				c.put(UUID.randomUUID().toString().substring(0, 20),
						UUID.randomUUID().toString().substring(0, random.nextInt(20)));
			}
			long tookEach = (System.nanoTime() - startEach) / 1000000;
			System.out.printf("Took %dms to send 100 messages\n", tookEach);
		}
		long took = (System.nanoTime() - start) / 1000000;
		System.out.printf("Took %dms for %d client(s) to send 100 messages each\n", took, nClients);

		start = System.nanoTime();
		for (KVClient c : clients) {
			long startEach = System.nanoTime();
			for (int i = 0; i < 100; i++) {
				c.get(UUID.randomUUID().toString().substring(0, 20));
			}
			long tookEach = (System.nanoTime() - startEach) / 1000000;
			System.out.printf("Took %dms to read 100 messages\n", tookEach);
		}
		took = (System.nanoTime() - start) / 1000000;
		System.out.printf("Took %dms for %d client(s) to read 100 messages each\n", took, nClients);

		start = System.nanoTime();
		for (KVClient c : clients) {
			long startEach = System.nanoTime();
			for (int i = 0; i < 100; i++) {
				c.delete(UUID.randomUUID().toString().substring(0, 20));
			}
			long tookEach = (System.nanoTime() - startEach) / 1000000;
			System.out.printf("Took %dms to delete 100 messages\n", tookEach);
		}
		took = (System.nanoTime() - start) / 1000000;
		System.out.printf("Took %dms for %d client(s) to delete 100 messages each\n", took, nClients);
	}
}

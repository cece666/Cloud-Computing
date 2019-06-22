package app_kvEcs;

import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cache.ServerCache;

/**
 * This class parses the input from the ECS and takes action accordingly.
 * 
 * @author uy Ha
 *
 */
public class ECSClient extends ECSCommunication {
    private static final Pattern INIT_SERVICE = Pattern.compile("initService\\s+(\\d+)\\s+(\\d+)\\s+([^\\s]+)");
    private static final Pattern START = Pattern.compile("start");
    private static final Pattern STOP = Pattern.compile("stop");
    private static final Pattern SHUTDOWN = Pattern.compile("shutDown");
    private static final Pattern ADD_NODE = Pattern.compile("addNode\\s+(\\d+)\\s+([^\\s]+)");
    private static final Pattern REMOVE_NODE = Pattern.compile("removeNode");
    private static final Pattern EXIT = Pattern.compile("exit");
    private static final Pattern HELP = Pattern.compile("help");
    private static Logger logger = LogManager.getLogger("kvECS");

    public ECSClient() throws IOException {
	super();
    }

    /**
     * This method recognize the command entered by the user, matches the command to
     * the predefined command patterns and calls the method corresponding to the
     * command entered or help() if the command is unknown.
     * 
     * @param command
     */
    public void processCommand(String command) {
	logger.info(command);
	Matcher matcher;
	try {
	    if ((matcher = INIT_SERVICE.matcher(command)).find()) {
		String numberOfNodes = matcher.group(1);
		String cacheSize = matcher.group(2);
		String displacementStrategy = matcher.group(3);
		initService(numberOfNodes, cacheSize, displacementStrategy);
	    } else if ((matcher = START.matcher(command)).find()) {
		start();
	    } else if ((matcher = STOP.matcher(command)).find()) {
		stop();
	    } else if ((matcher = SHUTDOWN.matcher(command)).find()) {
		shutDown();
	    } else if ((matcher = ADD_NODE.matcher(command)).find()) {
		String cacheSize = matcher.group(1);
		String displacementStrategy = matcher.group(2);
		addNode(cacheSize, displacementStrategy);
	    } else if ((matcher = REMOVE_NODE.matcher(command)).find()) {
		removeNode();
	    } else if ((matcher = EXIT.matcher(command)).find()) {
		System.out.println("Exiting");
		System.exit(0);
	    } else if ((matcher = HELP.matcher(command)).find()) {
		help();
	    } else {
		System.out.println("Unregconized command");
		help();
	    }
	} catch (IOException | IllegalArgumentException | InterruptedException e) {
	    System.out.println(e.getMessage());
	    logger.error(e);
	}
    }

    /**
     * This method specific numbers of initServers
     * 
     * @param numberOfNodes
     * @param cacheSize
     * @param displacementStrategy
     * @throws NumberFormatException
     * @throws IOException
     * @throws InterruptedException
     */
    private void initService(String numberOfNodes, String cacheSize, String displacementStrategy)
	    throws NumberFormatException, IOException, InterruptedException {
	if (!displacementStrategy.toUpperCase().equals(ServerCache.FIFO)
		&& !displacementStrategy.toUpperCase().equals(ServerCache.LRU)
		&& !displacementStrategy.toUpperCase().equals(ServerCache.LFU)) {
	    throw new IllegalArgumentException(String.format("Cache strategy has to either %s, %s or %s, received %s",
		    ServerCache.FIFO, ServerCache.LRU, ServerCache.LFU, displacementStrategy));
	}

	initService(Integer.parseInt(numberOfNodes), Integer.parseInt(cacheSize), displacementStrategy);
    }

    /**
     * This method add one server node
     * 
     * @param cacheSize
     * @param displacementStrategy
     * @throws NumberFormatException
     * @throws IOException
     * @throws InterruptedException
     */
    private void addNode(String cacheSize, String displacementStrategy)
	    throws NumberFormatException, IOException, InterruptedException {
	if (!displacementStrategy.toUpperCase().equals(ServerCache.FIFO)
		&& !displacementStrategy.toUpperCase().equals(ServerCache.LRU)
		&& !displacementStrategy.toUpperCase().equals(ServerCache.LFU)) {
	    throw new IllegalArgumentException(String.format("Cache strategy has to either %s, %s or %s, received %s",
		    ServerCache.FIFO, ServerCache.LRU, ServerCache.LFU, displacementStrategy));
	}
	addNode(new ServerConfig(Integer.parseInt(cacheSize), displacementStrategy));
    }

    /**
     * This method prints out the help manual
     */
    public void help() {
	final String helpString = "Commands:\n"
		+ "initService <numberOfnodes> <cacheSize> <displacement Strategy> \t Initializes the specified number of servers"
		+ "\nstart \t\t starts the servers" + "\nstop \t\t stops the servers"
		+ "\nshutDown shuts down the servers."
		+ "\naddNode <cacheSize> <displacementStrategy>\t\tAdds a server to the existing servers"
		+ "\nremoveNode \t Removes a random server from the existing ones" + "\nexit \t Exit the program"
		+ "\nhelp \t Print this message";
	System.out.println(helpString);
    }

    public static void main(String[] args) {
	try (Scanner scanner = new Scanner(System.in)) {
	    try {
		ECSClient client = new ECSClient();

		client.startRecovery();

		System.out.print("ECS>");
		while (scanner.hasNextLine()) {
		    client.processCommand(scanner.nextLine());
		    System.out.print("ECS>");
		}
		client.shutDown();
	    } catch (IOException e) {
		logger.warn(e);
		System.out.println(e.getMessage());
	    }
	}
    }
}

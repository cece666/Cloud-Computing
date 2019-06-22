package app_kvEcs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import common.messages.KVMessage;
import common.messages.StatusType;
import common.metadata.MDEntry;
import common.util.MarshallUtils;

/**
 * This class handles the case when a server reports another dead server
 * 
 * @Author Uy Ha
 */

public class ServerReport implements Runnable {

	private final Consumer<MDEntry> failNodeHandler;
	private final ExecutorService executorService = Executors.newCachedThreadPool();
	private final ServerSocket listener;
	private static Logger logger = LogManager.getLogger("kvECS");

	public ServerReport(ServerSocket listener, Consumer<MDEntry> failNodeHandler) throws IOException {
		this.failNodeHandler = failNodeHandler;
		this.listener = listener;
	}

	@Override
	public void run() {
		while (true) {
			try {
				Runnable messageProcessor = processMessage(listener.accept());
				executorService.submit(messageProcessor);
			} catch (IOException e) {
				logger.error(e);
			} catch (Exception e) {
				logger.error(e);
			}
		}
	}

	/**
	 * Adds the dead server to the failed node handler
	 * 
	 * @param rawSocket the server socket from which the dead server is reported
	 */
	private Runnable processMessage(Socket rawSocket) {
		return () -> {
			try (Socket socket = rawSocket) {
				KVMessage message = MarshallUtils.readFromServer(socket);
				if (message.getStatus() != StatusType.DEAD_SERVER) {
					return;
				}

				MDEntry entry = MDEntry.fromValueString(message.getValue(0));
				failNodeHandler.accept(entry);
			} catch (IOException e) {
				logger.error(e);
			}
		};
	}

}

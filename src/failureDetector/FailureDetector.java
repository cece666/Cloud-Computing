package failureDetector;

import java.io.IOException;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import app_kvServer.ServerState;
import common.messages.KVMessage;
import common.messages.KeyValue;
import common.messages.ServerMessage;
import common.messages.StatusType;
import common.metadata.AddressPort;
import common.metadata.MDEntry;
import common.util.MarshallUtils;

/**
 * This class keeps pinging servers to see if they're dead or alive
 * 
 * @Author Aleena Yunus
 */

public class FailureDetector implements Runnable {

	private static final Logger logger = LogManager.getLogger("kvServer");

	private final ServerState state;

	public FailureDetector(ServerState state) {
		this.state = state;
	}

	/**
	 * Keeps pinging continuously
	 */
	@Override
	public void run() {
		while (true) {
			pingChildren();
		}
	}

	/**
	 * Retreieves the servers to be pinged and pings them
	 */
	private void pingChildren() {
		Queue<MDEntry> children = state.getMetadata().getPingedTargets(state.getServerMeta());

		try {
			while (!children.isEmpty()) {
				MDEntry child = children.remove();
				if (!ping(child)) {
					children.addAll(state.getMetadata().getPingedTargets(child));
					report(child);
				}
			}
			TimeUnit.MINUTES.sleep(2);
		} catch (InterruptedException | IOException e) {
			logger.warn(e);
		}
	}

	/**
	 * Sends message to the pinged target to see if it is alive
	 * 
	 * @param child MDEntry of the server to be pinged
	 * @return boolean true if child alive
	 */
	private boolean ping(MDEntry child) throws IOException {
		try (Socket socket = new Socket(child.addressPort.address, child.addressPort.port)) {
			MarshallUtils.writeToServer(new ServerMessage(StatusType.IDENTIFY, new KeyValue("", null)), socket);
			MarshallUtils.writeToServer(new ServerMessage(StatusType.PING, new KeyValue("", null)), socket);
			KVMessage response = MarshallUtils.readFromServer(socket);

			return response != null && response.getStatus() == StatusType.PONG;
		} catch (IOException e) {
			logger.error(e);
			return false;
		}
	}

	/**
	 * Reports the dead server to the ecs
	 * 
	 * @param child MDEntry of the server to be pinged
	 * @return Nothing
	 */
	private void report(MDEntry child) throws IOException {
		AddressPort addressPort = state.getAddressPort();
		try (Socket socket = new Socket(addressPort.address, addressPort.port)) {
			MarshallUtils.writeToServer(new ServerMessage(StatusType.DEAD_SERVER, new KeyValue("", child.valueString())),
					socket);
		} catch (IOException e) {
			logger.error(e);
		}
	}
}

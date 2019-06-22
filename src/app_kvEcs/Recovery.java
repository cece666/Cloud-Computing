package app_kvEcs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import common.metadata.MDEntry;

/**
 * This class handles the recovery process of dead nodes
 * 
 * @Author Uy Ha
 */

public class Recovery implements Runnable {

	private static final int LISTEN_PORT = 4999;

	private final Consumer<Set<MDEntry>> recover;
	private final Set<MDEntry> failNodes = Collections.synchronizedSet(new HashSet<>());
	private final ServerSocket listener;
	private static Logger logger = LogManager.getLogger("kvECS");

	public Recovery(Consumer<Set<MDEntry>> recover) throws IOException {
		this.recover = recover;
		this.listener = new ServerSocket(LISTEN_PORT);
		new Thread(new ServerReport(listener, failNodes::add)).start();
	}

	/**
	 * Continuously runs to accept failed nodes and recover them every 5 mins
	 * 
	 */
	@Override
	public void run() {
		while (true) {
			try {
				recover.accept(failNodes);
				failNodes.clear();
				TimeUnit.MINUTES.sleep(10);

			} catch (InterruptedException e) {
				logger.error(e);
			}
		}

	}

	public InetAddress getAddress() {
		return listener.getInetAddress();
	}

	public int getPort() {
		return listener.getLocalPort();
	}

}

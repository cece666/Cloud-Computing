package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KeyValue;
import common.messages.ServerMessage;
import common.messages.StatusType;
import common.util.MarshallUtils;

/**
 * This class is the superclass for the client and ECS handlers
 * 
 * @author Uy Ha
 */
public abstract class BaseHandler implements Runnable {
	protected final Socket socket;
	protected final ExecutorService service;
	protected final Logger logger;

	public BaseHandler(Socket socket, ExecutorService service, Logger logger) {
		this.socket = socket;
		this.service = service;
		this.logger = logger;
	}

	public void run() {
		try {
			preRun();
			while (!socket.isClosed()) {
				try {
					if (!readAndProcess())
						break;
				} catch (IllegalStateException | IllegalArgumentException e) {
					logger.error(e);
					MarshallUtils.writeToServer(failMessage(e), socket);
				} catch (IOException e) {
					logger.error("Something happened when reading and processing client's request", e);
				} catch (Exception e) {

				}
			}
		} catch (Exception e) {
			logger.error("Fail to clean up client handler", e);
		} finally {
			try {
				socket.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
	}

	/**
	 * Read the message sent by client and submit the process procedure to a thread
	 * pool, then wait and get the result value and send it back to the client
	 * 
	 * @return boolean if the stream is not close
	 * @throws IllegalStateException
	 *             if message is null
	 * @throws IllegalArgumentException
	 *             if message containing a null key
	 * @throws IOException
	 *             if something happened to the socket
	 */
	protected boolean readAndProcess() throws IllegalStateException, IllegalArgumentException, IOException {
		final KVMessage requestedMessage = MarshallUtils.readFromServer(socket);
		if (requestedMessage == null) {
			return false;
		}

		service.submit(() -> {
			KVMessage result = directMessage(requestedMessage);
			try {
				MarshallUtils.writeToServer(result, socket);
			} catch (IllegalArgumentException | IOException e) {
				logger.error(e);
			}
		});
		return true;

	}

	protected abstract void preRun() throws IOException;

	protected abstract KVMessage directMessage(KVMessage message);

	/**
	 * This method from a ServerMessage with status:FAIL
	 * 
	 * @param e
	 * @return
	 */
	private KVMessage failMessage(Exception e) {
		return new ServerMessage(StatusType.FAIL, new KeyValue("", e.getMessage()));
	};
}

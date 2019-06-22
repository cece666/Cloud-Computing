package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import app_kvServer.KVServer.Status;
import common.messages.KVMessage;
import common.messages.KeyValue;
import common.messages.ServerMessage;
import common.messages.StatusType;
import common.util.MarshallUtils;

/**
 * This class calls the appropriate handler depending upon the status of the
 * message
 * 
 * @author Uy Ha
 */
public class HandlerRouter implements Runnable {
	private static final Logger logger = LogManager.getLogger("kvServer");

	private final Socket socket;
	private final ServerState state;
	private final ExecutorService service;
	private final Runnable shutDown;

	public HandlerRouter(Socket socket, ServerState state, ExecutorService executorService, Runnable shutDown) {
		this.socket = socket;
		this.state = state;
		this.service = executorService;
		this.shutDown = shutDown;
	}

	/**
	 * Checks the status of the message and calls ecs, client or server handler
	 * accordingly
	 */
	@Override
	public void run() {
		try {
			KVMessage message = MarshallUtils.readFromServer(socket);
			switch (message.getSource()) {
			case ECS:
				ECSHandler ecsHandler = new ECSHandler(socket, state, shutDown);
				service.execute(ecsHandler);
				break;
			case CLIENT:
				if (!readyForNormalClient()) {
					KVMessage stopMessage = new ServerMessage(StatusType.SERVER_STOPPED,
							new KeyValue("", "Server is not ready to handle client requests"));
					MarshallUtils.writeToServer(stopMessage, socket);
					return;
				}
				ClientHandler clientHandler = new ClientHandler(socket, state);
				service.execute(clientHandler);
				break;
			case SERVER:
				ServerHandler serverHanlder = new ServerHandler(socket, state);
				service.execute(serverHanlder);
				break;
			default:
				logger.warn(message.getStatus().name());
				KVMessage stopMessage = new ServerMessage(StatusType.FAIL, new KeyValue("", "Unknown Identity"));
				MarshallUtils.writeToServer(stopMessage, socket);
				break;
			}
		} catch (IOException e) {
			logger.error(e);
		}
	}

	/**
	 * This method estimate whether the server is ready for connection
	 * 
	 * @return boolean whether the server is active.
	 */
	private boolean readyForNormalClient() {
		return state.getServerStatus() != Status.UNINITIALIZED && state.getServerStatus() != Status.STOPPED;
	}
}

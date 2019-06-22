package client;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.function.Consumer;

import common.metadata.AddressPort;

public class ConnectionManager implements AutoCloseable {
    private final HashMap<AddressPort, Socket> connectionMap = new HashMap<>();

    public Socket getConnection(String address, int port, Consumer<Socket> newConnectionHandler,
	    Consumer<Socket> identifier) throws IOException {
	return getConnection(new AddressPort(address, port), newConnectionHandler, identifier);
    }

    public Socket getConnection(String address, int port) throws IOException {
	return getConnection(new AddressPort(address, port));
    }

    public Socket getConnection(AddressPort addressPort) throws IOException {
	return getConnection(addressPort, null, null);
    }

    /**
     * Build connection to server by addressPort, and put the connected servers to
     * connectionMap.
     * 
     * @param addressPort
     * @param newConnectionHandler
     * @param identifier
     * @return Socket connection
     * @throws IOException
     */
    public Socket getConnection(AddressPort addressPort, Consumer<Socket> newConnectionHandler,
	    Consumer<Socket> identifier) throws IOException {
	Socket resultConnection = connectionMap.get(addressPort);

	if (resultConnection == null) {
	    resultConnection = new Socket(addressPort.address, addressPort.port);
	    connectionMap.put(addressPort, resultConnection);

	    if (identifier != null)
		identifier.accept(resultConnection);

	    if (newConnectionHandler != null)
		newConnectionHandler.accept(resultConnection);
	}

	return resultConnection;
    }

    /**
     * remove connection by addressPort, and delete the server from conectionMap
     * 
     * @param addressPort
     * @return
     */
    public Socket remove(AddressPort addressPort) {
	return connectionMap.remove(addressPort);
    }

    @Override
    public void close() throws IOException {
	for (Socket socket : connectionMap.values()) {
	    socket.close();
	}
    }
}

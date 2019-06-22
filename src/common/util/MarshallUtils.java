package common.util;

import static common.util.ConvertUtils.bytesToInt;
import static common.util.ConvertUtils.concatArray;
import static common.util.ConvertUtils.intToBytes;
import static common.util.ConvertUtils.totalLength;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import common.messages.ClientMessage;
import common.messages.ECSMessage;
import common.messages.KVMessage;
import common.messages.KeyValue;
import common.messages.ServerMessage;
import common.messages.Source;
import common.messages.StatusType;

/**
 * This class takes responsible of transformation between int and byte.
 * 
 * @author Jiaxi Zhao
 *
 */
public class MarshallUtils {
	private static final int OFFSET = 12;

	/**
	 * This method receive a KVMessage and turn it into byte array in a certain
	 * sequence.
	 * 
	 * @param KVMessage message
	 * @return buffer.array
	 * @throws IOException
	 */
	public static byte[] marshall(KVMessage message) throws IOException {

		Source source = message.getSource();
		StatusType status = message.getStatus();
		ArrayList<KeyValue> pairs = message.getPairs();

		byte[] sourceBytes = intToBytes(source.ordinal());
		byte[] statusBytes = intToBytes(status.ordinal());
		byte[] pairsLengthBytes = intToBytes(pairs.size());
		byte[] pairsBytes = pairsBytes(pairs);

		int totalLength = totalLength(sourceBytes, statusBytes, pairsLengthBytes, pairsBytes);
		byte[] messageLengthBytes = intToBytes(totalLength);

		return concatArray(messageLengthBytes, sourceBytes, statusBytes, pairsLengthBytes, pairsBytes);

	}

	/**
	 * This method receive a byte array and turn it into a KVMessage.
	 * 
	 * @param rawMessage
	 * @return KVMessage
	 */
	public static KVMessage unmarshall(byte[] rawMessage) {
		ByteBuffer buffer = ByteBuffer.wrap(rawMessage);

		int sourceInt = buffer.getInt();
		int statusInt = buffer.getInt();
		int pairsLength = buffer.getInt();

		Source source = Source.fromOrdinal(sourceInt);
		StatusType status = StatusType.fromOrdinal(statusInt);

		byte[] pairsBytes = Arrays.copyOfRange(rawMessage, OFFSET, rawMessage.length);

		switch (source) {
		case CLIENT:
			return new ClientMessage(status, pairsLength, pairsBytes);
		case ECS:
			return new ECSMessage(status, pairsLength, pairsBytes);
		case SERVER:
			return new ServerMessage(status, pairsLength, pairsBytes);
		default:
			throw new IllegalArgumentException("Unregconized source of message");

		}
	}

	public static byte[] pairsBytes(ArrayList<KeyValue> pairs) throws IOException {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();

		for (KeyValue pair : pairs) {
			stream.write(pairBytes(pair));
		}

		return stream.toByteArray();
	}

	public static byte[] pairBytes(KeyValue pair) {
		if (pair.key == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}

		byte[] key = pair.key.getBytes();
		byte[] keyLengthBytes = intToBytes(key.length);

		byte[] valueLengthBytes = intToBytes(-1);
		byte[] value = new byte[0];
		if (pair.value != null) {
			value = pair.value.getBytes();
			valueLengthBytes = intToBytes(value.length);
		}

		return concatArray(keyLengthBytes, valueLengthBytes, key, value);
	}

	public static KeyValue pairFromBytes(byte[] bytes) {
		return pairFromBytes(ByteBuffer.wrap(bytes));
	}

	private static KeyValue pairFromBytes(ByteBuffer buffer) {
		int keyLength = buffer.getInt();
		int valueLength = buffer.getInt();

		byte[] keyBytes = new byte[keyLength];
		buffer.get(keyBytes);
		String key = new String(keyBytes);

		String value = null;
		if (valueLength >= 0) {
			byte[] valueBytes = new byte[valueLength];
			buffer.get(valueBytes);
			value = new String(valueBytes);
		}

		return new KeyValue(key, value);
	}

	public static ArrayList<KeyValue> pairsFromBytes(int pairsNum, byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		ArrayList<KeyValue> result = new ArrayList<>(pairsNum);

		for (int i = 0; i < pairsNum; i++) {
			result.add(pairFromBytes(buffer));
		}

		return result;
	}

	/**
	 * read KVMessage from server.
	 * 
	 * @param socket
	 * @return
	 * @throws IOException
	 */
	public static KVMessage readFromServer(Socket socket) throws IOException {
		InputStream istream = socket.getInputStream();

		byte[] rawMessageLength = new byte[4];
		if (istream.read(rawMessageLength) < 0) {
			return null;
		}

		int messageLength = bytesToInt(rawMessageLength);
		byte[] rawMessage = new byte[messageLength];
		if (istream.read(rawMessage) < 0) {
			return null;
		}

		return unmarshall(rawMessage);
	}

	/**
	 * write KVMessage to server
	 * 
	 * @param message
	 * @param socket
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public static void writeToServer(KVMessage message, Socket socket) throws IllegalArgumentException, IOException {
		socket.getOutputStream().write(marshall(message));
	}

}
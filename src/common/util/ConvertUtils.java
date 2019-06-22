package common.util;

import static java.util.stream.Stream.of;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;

public class ConvertUtils {
	/**
	 * Turn an integer to a byte array
	 * 
	 * @param source the source integer
	 * @return the byte array representation of the source integer
	 */
	public static byte[] intToBytes(int source) {
		return ByteBuffer.allocate(4).putInt(source).array();
	}

	public static int totalLength(byte[]... arrays) {
		return of(arrays).mapToInt((a) -> a.length).sum();
	}

	public static byte[] concatArray(byte[]... arrays) {
		int totalLength = totalLength(arrays);

		ByteBuffer buffer = ByteBuffer.allocate(totalLength);
		of(arrays).forEach((a) -> buffer.put(a));

		return buffer.array();
	}

	/**
	 * Turn a byte array to an integer
	 * 
	 * @param bytes the byte array representation
	 * @return the integer represented by the array
	 */
	public static int bytesToInt(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getInt();
	}

	public static byte[] emptyIfNull(String value) {
		return value == null ? new byte[] {} : value.getBytes();
	}

	public static byte[] sumBytes(int... elements) {
		return intToBytes(IntStream.of(elements).sum());
	}
}

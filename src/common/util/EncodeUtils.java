package common.util;

import static common.util.ConvertUtils.bytesToInt;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class EncodeUtils {
	public static byte[] encodeSequence(String... members) {
		int dataLength = Arrays.stream(members).mapToInt(member -> member.length()).sum();
		int membersNum = members.length;

		int totalLength = (membersNum + 1) * 4 + dataLength;

		ByteBuffer buffer = ByteBuffer.allocate(totalLength);
		buffer.putInt(membersNum);
		Arrays.stream(members).mapToInt(member -> member.length()).forEach(length -> buffer.putInt(length));
		Arrays.stream(members).forEach(member -> buffer.put(member.getBytes()));

		return buffer.array();
	}

	public static String[] decodeSequence(InputStream istream) throws IOException {
		byte[] intBytes = new byte[4];
		if (istream.read(intBytes) == -1) {
			return null;
		}
		int membersNum = bytesToInt(intBytes);

		byte[][] byteBuffers = new byte[membersNum][];

		for (int i = 0; i < membersNum; i++) {
			intBytes = new byte[4];
			if (istream.read(intBytes) == -1) {
				throw new IOException("Stream is closed");
			}
			int length = bytesToInt(intBytes);
			byteBuffers[i] = new byte[length];
		}

		for (byte[] byteBuffer : byteBuffers) {
			istream.read(byteBuffer);
		}

		return Arrays.stream(byteBuffers).map(String::new).toArray(String[]::new);
	}
}

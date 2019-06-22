package app_kvDatabase;

import static common.hash.Hash.compare;
import static common.hash.Hash.hash;
import static common.util.ConvertUtils.bytesToInt;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class Account extends BaseEntity<String, byte[]> {
	private String username;
	private byte[] password;

	public Account(String username, byte[] password) {
		this.username = username;
		this.password = password;
	}

	public Account(String username, String password) {
		this(username, hash(password));
	}

	public Account() {

	}

	/*
	 * This method return password (non-Javadoc)
	 * 
	 * @see app_kvDatabase.BaseEntity#getValue()
	 */
	public byte[] getValue() {
		return password;
	}

	/**
	 * This method set password and hash it.
	 * 
	 * @param password
	 */
	public void setValue(String password) {
		this.password = hash(password);
	}

	/*
	 * This method return username (non-Javadoc)
	 * 
	 * @see app_kvDatabase.BaseEntity#getKey()
	 */
	public String getKey() {
		return username;
	}

	/**
	 * this method set username.
	 * 
	 * @param username
	 */
	public void setKey(String username) {
		this.username = username;
	}

	/**
	 * This method estimate whether the password is correct
	 * 
	 * @param password
	 * @return
	 */
	public boolean samePassword(byte[] password) {
		return compare(this.password, password) == 0;
	}

	@Override
	public byte[] marshall() {
		int dataLength = username.length() + password.length;

		int totalLength = 8 + dataLength; // 4 bytes for username length and 4 bytes for password length

		ByteBuffer buffer = ByteBuffer.allocate(totalLength);
		buffer.putInt(username.length());
		buffer.putInt(password.length);
		buffer.put(username.getBytes());
		buffer.put(password);

		return buffer.array();
	}

	@Override
	public boolean populate(InputStream istream) throws IOException {
		byte[] usernameLengthBytes = new byte[4];
		byte[] passwordLengthBytes = new byte[4];

		if (istream.read(usernameLengthBytes) == -1)
			return false;
		istream.read(passwordLengthBytes);

		byte[] usernameBytes = new byte[bytesToInt(usernameLengthBytes)];
		byte[] passwordBytes = new byte[bytesToInt(passwordLengthBytes)];
		istream.read(usernameBytes);
		istream.read(passwordBytes);

		username = new String(usernameBytes);
		password = passwordBytes;

		return true;
	}
}

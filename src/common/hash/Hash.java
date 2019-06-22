package common.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This class calculates the hash values of the servers and keys
 * 
 * @author Aleena Yunus
 */
public class Hash {

	private static final String mode = "MD5";
	private static MessageDigest mDigest = init();

	public static MessageDigest init() {
		try {
			return MessageDigest.getInstance(mode);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Gives the byte array containing the hash value of a string
	 * 
	 * @param value the value to be hashed
	 * @return byte[] the byte array containing the hash
	 */
	public static byte[] hash(String value) {
		mDigest.reset();
		byte[] result = value.getBytes();
		result = mDigest.digest(result);
		return result;
	}

	/**
	 * Compares two hash values
	 * 
	 * @param hash1 hash value of the first arg
	 * @param hash2 hash value of the second arg
	 * @return int negative if hash1 is smaller, positive if hash1 is greater and 0
	 *         if they are equal
	 */
	public static int compare(byte[] hash1, byte[] hash2) {

		if (hash1.length != hash2.length) {
			return Integer.compare(hash1.length, hash2.length);
		}

		for (int i = 0; i < hash1.length; i++) {
			if (hash1[i] != hash2[i])
				return Byte.compare(hash1[i], hash2[i]);
		}

		return 0;
	}

	/**
	 * Checks if a given hash value is within a range
	 * 
	 * @param value the value to be checked
	 * @param lower lower bound
	 * @param upper upper bound
	 * @return boolean true or false depending upon whether it is in the range or
	 *         not
	 */
	public static boolean in(byte[] value, byte[] lower, byte[] upper) {
		if (compare(lower, upper) > 0)
			return compare(value, lower) > 0 || compare(value, upper) <= 0;

		if (compare(lower, upper) == 0)
			return true;

		return compare(value, lower) > 0 && compare(value, upper) <= 0;
	}
}

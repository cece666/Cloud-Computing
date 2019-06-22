package app_kvDatabase;

import java.io.IOException;
import java.io.InputStream;

public abstract class BaseEntity<K, V> {

	/**
	 * This method turn corresponding message into byte array
	 * 
	 * @return
	 */
	public abstract byte[] marshall();

	/**
	 * This method turn the byte message in stream into corresponding db message.
	 * 
	 * @param istream
	 * @return
	 * @throws IOException
	 */
	public abstract boolean populate(InputStream istream) throws IOException;

	/**
	 * This method return key value of the Entity
	 * 
	 * @return
	 */
	public abstract K getKey();

	/**
	 * This method return value value of the Entity
	 * 
	 * @return
	 */
	public abstract V getValue();

}

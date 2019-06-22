package app_kvDatabase;

import static common.hash.Hash.hash;

import java.io.IOException;

import common.hash.Hash;

public class UserDatabase extends BaseDatabase<String, byte[], Account> {

	/**
	 * 
	 * @param dbFile
	 * 
	 */
	public UserDatabase(String dbFile) throws IOException {
		super(dbFile);
	}

	/**
	 * put new key and value in HashMap and write data to stream. return the key's
	 * corresponding old value.
	 * 
	 * @param username
	 * @param password
	 * @throws IOException
	 */
	public void put(String username, String password) throws IOException {
		put(username, hash(password));
	}

	/**
	 * This method estimate whether username matches password.
	 * 
	 * @param username
	 * @param password
	 * @return
	 */
	public boolean authenticate(String username, String password) {
		return exists(username) && Hash.compare(get(username), hash(password)) == 0;
	}

	/**
	 * This method estimate whether the username exists.
	 * 
	 * @param username
	 * @return
	 */
	public boolean exists(String username) {
		return db.keySet().contains(username);
	}

	@Override
	protected Account newEntity() {
		return new Account();
	}

	@Override
	protected Account newEntity(String key, byte[] value) {
		return new Account(key, value);
	}
}

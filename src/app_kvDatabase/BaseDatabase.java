/**
 * 
 */
package app_kvDatabase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import common.hash.Hash;

/**
 * This class handle the mapping between key and value. and offer the methods of
 * reading, writing, putting, removing, getting values from files.
 * 
 * @author Uy Ha
 *
 */

public abstract class BaseDatabase<K, V, T extends BaseEntity<K, V>> {
	protected static final Logger logger = LogManager.getLogger("kvServer");

	protected final File storageFile;
	protected final HashMap<K, V> db;
	protected final ExecutorService service;

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	/**
	 * 
	 * @param dbFile
	 * 
	 */
	public BaseDatabase(String dbFile) throws IOException {
		storageFile = new File(dbFile);
		storageFile.createNewFile();
		db = readData();
		service = Executors.newSingleThreadExecutor();
	}

	protected abstract T newEntity();

	protected abstract T newEntity(K key, V value);

	/**
	 * read data from stream and map keys and values from KVEntity.
	 * 
	 * @return kvMap
	 * @throws IOException
	 */
	protected HashMap<K, V> readData() throws IOException {
		try (InputStream istream = new FileInputStream(storageFile)) {

			HashMap<K, V> result = new HashMap<>();
			T entity = newEntity();

			while (entity.populate(istream)) {
				result.put(entity.getKey(), entity.getValue());
				entity = newEntity();
			}

			return result;
		}
	}

	/**
	 * map key and value from HashMap to KVEntity and write data to stream.
	 * 
	 * 
	 * @param kvmap
	 * @throws IOException
	 */

	protected Runnable writeData(HashMap<K, V> kvMap) {
		return () -> {
			try (OutputStream ostream = new FileOutputStream(storageFile)) {

				ArrayList<T> entities = new ArrayList<>(kvMap.size());
				kvMap.forEach((key, data) -> entities.add(newEntity(key, data)));
				for (T entity : entities) {
					ostream.write(entity.marshall());
				}
			} catch (IOException e) {
				BaseDatabase.logger.warn(e);
			}
		};
	}

	/**
	 * put new key and value in HashMap and write data to stream. return the key's
	 * corresponding old value.
	 * 
	 * 
	 * @param key,value
	 * @return oldValue
	 * @throws IOException
	 */
	public synchronized V put(K key, V value) {
		lock.writeLock().lock();
		V oldValue = db.put(key, value);
		lock.writeLock().unlock();

		service.submit(writeData(db));

		return oldValue;
	}

	/**
	 * remove key and corresponding value from HashMap. and write the new mapping to
	 * stream. return old value.
	 * 
	 * @param key
	 * @return oldValue
	 * @throws IOException
	 */
	public synchronized V remove(K key) {
		lock.writeLock().lock();
		V oldValue = db.remove(key);
		lock.writeLock().unlock();

		service.submit(writeData(db));

		return oldValue;
	}

	/**
	 * Retrieve the {@code value} associated with @{code key} in the database file
	 * 
	 * @param key the key of the pair
	 * @return the value of the pair
	 */
	public synchronized V get(K key) {
		lock.writeLock().lock();
		V value = db.get(key);
		lock.writeLock().unlock();
		return value;
	}

	/**
	 * Replicates the {@code value} associated with @{code key} in the database file
	 * of a server replica
	 * 
	 * @param pairs A list of key value pairs
	 * @return Nothing
	 * @throws IOException
	 */
	public synchronized void replicate(ArrayList<T> pairs) {
		lock.writeLock().lock();
		HashMap<K, V> refreshReplica = new HashMap<>();
		for (T pair : pairs) {
			refreshReplica.put(pair.getKey(), pair.getValue());
		}
		lock.writeLock().unlock();

		service.submit(writeData(refreshReplica));
	}

	/**
	 * Collect the data in the given range
	 * 
	 * @param start the starting index to collect data (exclusive)
	 * @param end   the ending index to collect data (inclusive)
	 * @return the collected data
	 */
	public HashMap<K, V> dataInRange(byte[] start, byte[] end) {
		HashMap<K, V> result = new HashMap<>();
		for (Map.Entry<K, V> entry : db.entrySet()) {
			if (Hash.in(Hash.hash(entry.getKey().toString()), start, end)) {
				result.put(entry.getKey(), entry.getValue());
			}
		}
		return result;
	}

	/**
	 * Returns the data from the storage file
	 * 
	 * @param Nothing
	 * @return A hashMap containing the key value pairs
	 */
	public HashMap<K, V> getData() {
		return db;
	}

	/**
	 * Returns the out of range data for a given start and end point
	 * 
	 * @param start a byte array containing the starting point's hash
	 * @param end   a byte array containing the ending point's hash
	 * @return A hashMap containing the out of range data
	 */
	public synchronized HashMap<K, V> dataOutOfRange(byte[] start, byte[] end) {
		HashMap<K, V> result = new HashMap<>();
		for (Map.Entry<K, V> entry : db.entrySet()) {
			if (!(Hash.in(Hash.hash(entry.getKey().toString()), start, end))) {
				result.put(entry.getKey(), entry.getValue());
			}
		}
		return result;
	}

}

package app_kvDatabase;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KVDatabase extends BaseDatabase<String, KVData, KVEntity> {

	public KVDatabase(String dbFile) throws IOException {
		super(dbFile);
	}

	@Override
	protected KVEntity newEntity() {
		return new KVEntity();
	}

	@Override
	protected KVEntity newEntity(String key, KVData value) {
		return new KVEntity(key, value);
	}

	/**
	 * This method remove expired data from db
	 * 
	 * @return An ArrayList<String> of expired keys.
	 */
	public synchronized List<String> removeExpiredData() {
		ArrayList<String> expiredKeys = new ArrayList<String>();

		for (Map.Entry<String, KVData> entry : db.entrySet()) {
			if (entry.getValue().delTime != null && entry.getValue().delTime.isBefore(LocalDateTime.now())) {
				expiredKeys.add(entry.getKey());
			}
		}

		expiredKeys.stream().forEach(this::remove);

		return expiredKeys;
	}

}

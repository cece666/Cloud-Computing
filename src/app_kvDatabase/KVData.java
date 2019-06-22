package app_kvDatabase;

import java.time.LocalDateTime;

public class KVData {
	public final String value;
	public final String owner;
	public final LocalDateTime delTime;

	public KVData(String value, String owner, LocalDateTime delTime) {
		this.value = value;
		this.owner = owner;
		this.delTime = delTime;
	}
}

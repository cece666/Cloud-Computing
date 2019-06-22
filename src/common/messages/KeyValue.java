package common.messages;

public class KeyValue {
	public final String key;
	public final String value;
	// public final String timelapse;

	public KeyValue(String key, String value) {

		this.key = key;
		this.value = value;
	}

	public KeyValue() {
		this.key = "";
		this.value = "";
	}
}

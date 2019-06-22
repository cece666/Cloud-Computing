package app_kvDatabase;

import static common.util.EncodeUtils.decodeSequence;
import static common.util.EncodeUtils.encodeSequence;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;

/**
 * This class handle the mapping between the binary in a file to a Java object
 * 
 * @author Uy Ha
 *
 */
public class KVEntity extends BaseEntity<String, KVData> {
	private String key;
	private KVData data;

	public KVEntity() {

	}

	/**
	 * @param key
	 * @param value
	 */
	public KVEntity(String key, KVData data) {
		this.key = key;
		this.data = data;
	}

	/**
	 * Read a the binary in the stream and turn it in a KVEnity, this function
	 * assumes that current cursor of the stream is the start of the binary and also
	 * the binary in the stream is valid.
	 * 
	 * 
	 * @param stream a stream containing the binary of the KVEntity
	 * @return the KVEntity contained in the stream
	 * @throws IOException
	 */
	// Message structure:userNameLength ---4 bytes; keyLength---4 bytes;
	// valueLength---4 bytes;
	// KVtimestamp; userName; Key; Value
	public boolean populate(InputStream istream) throws IOException {
		String[] fields = decodeSequence(istream);
		if (fields == null)
			return false;

		key = fields[0];
		data = new KVData(fields[1], fields[2], LocalDateTime.parse(fields[3]));
		return true;
	}

	/**
	 * This is the opposite counterpart of unmarshall, it turns an KVEntity into
	 * binary that unmarshall can turn it into KVEntity again
	 * 
	 * @param entity
	 * @return a byte[] containing the binary of KVEntity
	 */
	public byte[] marshall() {
		return encodeSequence(key, data.value, data.owner, data.delTime.toString());
	}

	public String getKey() {
		return key;
	}

	public KVData getValue() {
		return data;
	}
}

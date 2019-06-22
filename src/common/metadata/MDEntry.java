package common.metadata;

import common.hash.Hash;

/**
 * This class form MDEntry by reading from config file. and also offers methods
 * for mdentry estimation.
 * 
 * @author uy Ha
 *
 */
public class MDEntry {
    public final String nodeName;
    public final AddressPort addressPort;
    public final byte[] hashIndex;

    public MDEntry(String nodeName, String address, int port, byte[] hashIndex) throws IllegalArgumentException {
	this.nodeName = nodeName;
	this.addressPort = new AddressPort(address, port);
	this.hashIndex = hashIndex != null ? hashIndex
		: Hash.hash(String.format("%s:%d", this.addressPort.address, this.addressPort.port));
    }

    public MDEntry(String nodeName, String address, int port) {
	this(nodeName, address, port, null);
    }

    /**
     * change nodeName,address and port into one String
     * 
     * @return String in specific format
     */
    public String toConfigString() {
	return String.format("%s %s %d", nodeName, addressPort.address, addressPort.port);
    }

    /**
     * @return String in specific format
     */
    public String valueString() {
	return toConfigString();
    }

    /**
     * Convert String to MDEntry
     * 
     * @param configString
     * @return MDEntry
     */
    public static MDEntry fromConfigString(String configString) {
	String[] configs = configString.split("\\s+");
	if (configs.length != 3)
	    throw new IllegalArgumentException(String.format(
		    "Expecting a string with 3 fields seperated by space(s), received a string of %d field(s)",
		    configs.length));

	String nodeName = configs[0];
	String address = configs[1];
	int port = Integer.parseInt(configs[2]);

	return new MDEntry(nodeName, address, port);
    }

    public static MDEntry fromValueString(String valueString) {
	return fromConfigString(valueString);
    }

    /**
     * A utility function for creating a fake MDEntry for easy routing
     * 
     * @param startIndex the startIndex of the fake MDEntry
     * @return a fake MDEntry with only startIndex
     */
    public static MDEntry fakeEntry(byte[] hashIndex) {
	return new MDEntry(null, null, 0, hashIndex);
    }

    /**
     * compare hashIndex between two entries.
     * 
     * @param entry1
     * @param entry2
     * @return Boolean
     */
    public static int hashIndexComparator(MDEntry entry1, MDEntry entry2) {
	byte[] hash1 = entry1.hashIndex;
	byte[] hash2 = entry2.hashIndex;

	if (hash1.length != hash2.length) {
	    return Integer.compare(hash1.length, hash2.length);
	}

	for (int i = 0; i < hash1.length; i++) {
	    if (hash1[i] != hash2[i])
		return Byte.compare(hash1[i], hash2[i]);
	}

	return 0;
    }

    @Override
    public int hashCode() {
	return addressPort.hashCode();
    }

    @Override
    public boolean equals(Object other) {
	if (this == other)
	    return true;
	if (other == null)
	    return false;
	if (!(other instanceof MDEntry))
	    return false;

	MDEntry target = (MDEntry) other;
	return nodeName.equals(target.nodeName) && addressPort.equals(target.addressPort);
    }
}

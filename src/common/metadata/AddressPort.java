package common.metadata;

/**
 * This class offers methods for address message retrival.
 * 
 * @author uy Ha
 *
 */
public class AddressPort {
    public final String address;
    public final int port;

    public AddressPort(String address, int port) {
	this.address = address == null ? "" : address.trim();
	this.port = port;
    }

    @Override
    public String toString() {
	return String.format("%s:%d", address, port);
    }

    @Override
    public int hashCode() {
	return toString().hashCode();
    }

    @Override
    public boolean equals(Object other) {
	if (this == other)
	    return true;
	if (other == null)
	    return false;
	if (!(other instanceof AddressPort))
	    return false;

	AddressPort target = (AddressPort) other;
	return address.equals(target.address) && port == target.port;
    }
}

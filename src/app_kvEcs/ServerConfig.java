package app_kvEcs;

/**
 * This class saves the server configuration of cache size and strategy
 * 
 * @author Uy Ha
 */
public class ServerConfig {
	public final int cacheSize;
	public final String displacementStrategy;

	public ServerConfig(int cacheSize, String displacementStrategy) {
		this.cacheSize = cacheSize;
		this.displacementStrategy = displacementStrategy;
	}
}

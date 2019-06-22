package app_kvServer;

import static app_kvServer.KVServer.Status.UNINITIALIZED;

import java.net.InetAddress;

import app_kvDatabase.KVDatabase;
import app_kvDatabase.UserDatabase;
import app_kvServer.KVServer.Status;
import cache.ServerCache;
import common.metadata.AddressPort;
import common.metadata.MDEntry;
import common.metadata.MDTable;

/**
 * Defines the different elements that constitute the server state and their
 * setters and getters
 * 
 * @author Uy Ha
 */

public class ServerState {
	private int cacheSize;
	private String strategy;
	private KVDatabase db;
	private KVDatabase replica1;
	private KVDatabase replica2;
	private Status serverStatus = UNINITIALIZED;
	private InetAddress addr;
	private MDTable metadata;
	private ServerCache cache;
	private MDEntry serverMeta;
	private final AddressPort addressPort;
	private UserDatabase userDb;

	public ServerState(KVDatabase db, KVDatabase replica1, KVDatabase replica2, UserDatabase userDb,
			AddressPort addressPort) {
		this.db = db;
		this.replica1 = replica1;
		this.replica2 = replica2;
		this.userDb = userDb;
		this.addressPort = addressPort;
	}

	public int getCacheSize() {
		return cacheSize;
	}

	public void setCacheSize(int cacheSize) {
		this.cacheSize = cacheSize;
	}

	public String getStrategy() {
		return strategy;
	}

	public void setStrategy(String strategy) {
		this.strategy = strategy;
	}

	public KVDatabase getDb() {
		return db;
	}

	public void setDb(KVDatabase db) {
		this.db = db;
	}

	public KVDatabase getReplica1() {
		return replica1;
	}

	public void setReplica1(KVDatabase replica1) {
		this.replica1 = replica1;
	}

	public KVDatabase getReplica2() {
		return replica2;
	}

	public void setReplica2(KVDatabase replica2) {
		this.replica2 = replica2;
	}

	public Status getServerStatus() {
		return serverStatus;
	}

	public void setServerStatus(Status serverStatus) {
		this.serverStatus = serverStatus;
	}

	public InetAddress getAddr() {
		return addr;
	}

	public void setAddr(InetAddress addr) {
		this.addr = addr;
	}

	public MDTable getMetadata() {
		return metadata;
	}

	public void setMetadata(MDTable metadata) {
		this.metadata = metadata;
	}

	public ServerCache getCache() {
		return cache;
	}

	public void setCache(ServerCache cache) {
		this.cache = cache;
	}

	public MDEntry getServerMeta() {
		return serverMeta;
	}

	public void setServerMeta(MDEntry serverMeta) {
		this.serverMeta = serverMeta;
	}

	public AddressPort getAddressPort() {
		return addressPort;
	}

	public UserDatabase getUserDb() {
		return userDb;
	}

	public void setUserDb(UserDatabase userDb) {
		this.userDb = userDb;
	}

}

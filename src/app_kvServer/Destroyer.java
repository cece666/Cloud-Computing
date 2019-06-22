package app_kvServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Destroyer implements Runnable {

	private static final Logger logger = LogManager.getLogger("kvServer");

	private final ServerState state;

	public Destroyer(ServerState state) {
		this.state = state;
	}

	@Override
	public void run() {
		try {
			while (true) {
				for (String key : state.getDb().removeExpiredData()) {
					state.getCache().invalidate(key);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
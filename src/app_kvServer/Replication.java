package app_kvServer;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import app_kvDatabase.KVData;
import common.messages.KeyValue;
import common.metadata.MDEntry;

/**
 * This class sends the to be replicated data from co-ordinator to replica
 * 
 * @Author Aleena Yunus
 */
public class Replication implements Runnable {

	private static final Logger logger = LogManager.getLogger("kvServer");

	private final ServerState state;

	public Replication(ServerState state) {
		this.state = state;
	}

	/**
	 * This methods gets the data to be replicated from the database and then sends
	 * it to the two replicas every 5 mins
	 *
	 */
	@Override
	public void run() {
		while (true) {
			try {
				HashMap<String, KVData> dataToBeReplicated = state.getDb().getData();
				if (!dataToBeReplicated.isEmpty()) {
					MDEntry replica1 = state.getMetadata().getSuccessor(state.getServerMeta());
					MDEntry replica2 = state.getMetadata().getSuccessor(replica1);
					replicate(replica1, dataToBeReplicated);
					replicate(replica2, dataToBeReplicated);
				}
				TimeUnit.MINUTES.sleep(5);
			} catch (InterruptedException | IOException e) {
				logger.error(e);
			}
			logger.info("Data replicated.");
		}

	}

	/**
	 * This method sends the keyvalues to one of the replicas
	 * 
	 * @param destination        MDEntry of the target replica
	 * @param dataToBeReplicated hashmap of the data that needs to be sent
	 */
	private void replicate(MDEntry destination, HashMap<String, KVData> dataToBeReplicated)
			throws UnknownHostException, IOException {
		DataDistributor dataDistributor = new DataDistributor(destination);
		ArrayList<KeyValue> kvs = dataToBeReplicated.entrySet().stream()
				.flatMap(i -> Stream.of(new KeyValue(i.getKey(), i.getValue().value),
						new KeyValue("owner", i.getValue().owner),
						new KeyValue("delTime", i.getValue().delTime.toString())))
				.collect(Collectors.toCollection(ArrayList::new));
		dataDistributor.replicateData(kvs);
	}
}

package scratch;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Iterator;

import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.transport.CLIClient;
import de.tuberlin.dima.presslufthammer.transport.Coordinator;
import de.tuberlin.dima.presslufthammer.transport.Leaf;
import de.tuberlin.dima.presslufthammer.transport.Slave;
import de.tuberlin.dima.presslufthammer.transport.SlaveCoordinator;
import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.Type;

/**
 * Test the query handling by a Slave tree.
 * 
 * @author Aljoscha Krettek
 */
public class SlaveTest {
	private static final String HOST = "localhost";
	private static final int PORT = 44444;
	private static final String DATASOURCES = "src/main/example-data/DataSources.xml";
	private static final File LEAF_DATADIR = new File("data-dir");

	public static void main(String[] args) throws Exception {

		SlaveCoordinator coord = new SlaveCoordinator(PORT, DATASOURCES);
		coord.start();
		
		Slave slave1 = new Slave(HOST, PORT, LEAF_DATADIR);
		slave1.start();
		Thread.sleep(2000);// wait until slave1 has bound
		Slave slave2 = new Slave(HOST, PORT, LEAF_DATADIR);
		slave2.start();
		Slave slave3 = new Slave(HOST, PORT, LEAF_DATADIR);
		slave3.start();
//		Thread.sleep(2000);

		CLIClient client = new CLIClient(HOST, PORT);
		client.start();

		// client.main(new String[] { HOST, String.valueOf(PORT)});
		boolean running = true;
		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(System.in));
		while (running) {
			String line = bufferedReader.readLine();
			if (line.startsWith("x")) {
				running = false;
			} else {
				client.query("0:Document.DocId,Document.Name.Language.Code,Document.Name.Language.Country:Document:-1::");
//				client.query(line);
			}
		}

		client.stop();
		slave1.stop();
		slave2.stop();
		slave3.stop();
		coord.stop();
	}
}

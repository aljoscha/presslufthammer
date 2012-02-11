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
		
		Slave slave_0 = new Slave(HOST, PORT, LEAF_DATADIR);
		slave_0.start();
		Thread.sleep(5000);// wait until slave1 has bound
		Slave slave_l0 = new Slave(HOST, PORT, LEAF_DATADIR);
		Slave slave_r0 = new Slave(HOST, PORT, LEAF_DATADIR);
		slave_l0.start();
		Thread.sleep(5000);// wait until slave1 has bound
		slave_r0.start();
		Thread.sleep(5000);// wait until slave1 has bound
		Slave slave_l1 = new Slave(HOST, PORT, LEAF_DATADIR);
		Slave slave_l2 = new Slave(HOST, PORT, LEAF_DATADIR);
		Slave slave_r1 = new Slave(HOST, PORT, LEAF_DATADIR);
		Slave slave_r2 = new Slave(HOST, PORT, LEAF_DATADIR);
//		slave_l1.start();
//		slave_l2.start();
//		slave_r1.start();
//		slave_r2.start();
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
		slave_l1.stop();
		slave_l2.stop();
		slave_r2.stop();
		slave_r1.stop();
		slave_l0.stop();
		slave_r0.stop();
		slave_0.stop();
		coord.stop();
	}
}

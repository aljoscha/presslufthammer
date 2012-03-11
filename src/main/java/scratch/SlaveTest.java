package scratch;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.query.parser.QueryParser;
import de.tuberlin.dima.presslufthammer.query.parser.QueryParser.ParseError;
import de.tuberlin.dima.presslufthammer.transport.CLIClient;
import de.tuberlin.dima.presslufthammer.transport.Slave;
import de.tuberlin.dima.presslufthammer.transport.SlaveCoordinator;
import de.tuberlin.dima.presslufthammer.transport.messages.QueryMessage;

/**
 * Test the query handling by a Slave tree.
 * 
 * @author feichh
 * @author Aljoscha Krettek
 */
public class SlaveTest {
	private static final Logger log = LoggerFactory.getLogger("SlaveTest");
	private static final String HOST = "localhost";
	private static final int PORT = 44444;
	private static final String DATASOURCES = "src/main/example-data/DataSources.xml";
	private static final File LEAF_DATADIR = new File("data-dir");
	private static final int NUM_SLAVES = 7;
	private static final int SLAVE_DEGREE = 2;

	public static void main(String[] args) throws Exception {

		log.info("Test Starting");
		SlaveCoordinator coord = new SlaveCoordinator(PORT, DATASOURCES);
		coord.start();

		List<Slave> slaves = new ArrayList<Slave>();
		Slave slave_0 = new Slave(SLAVE_DEGREE, HOST, PORT, LEAF_DATADIR,
				DATASOURCES);
		slave_0.start();
		slaves.add(0, slave_0);
		log.info("Waiting for the coordinator to accept the root node.");
		Thread.sleep(5000);// wait until slave_0 has been established as root
		for (int i = 1; i < NUM_SLAVES; i++) {
			Slave s = new Slave(SLAVE_DEGREE, HOST, PORT, LEAF_DATADIR,
					DATASOURCES);
			s.start();
			slaves.add(s);
			// Thread.sleep(1000);
		}
		log.info("{} Slaves have been added.", NUM_SLAVES);
		Thread.sleep(2000);
		log.info("Starting a client to send queries.");

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
				// } else if (line.startsWith("kill")) {
				// // int random = (int) (Math.random() * NUM_SLAVES);
				// slaves.get(slaves.size() - 1).stop();
			} else {
				try {
					Query query = QueryParser
							.parse("SELECT * FROM Document WHERE Document.Name.Url==\"http://A\"");
					System.out.println("QUERY: " + query);
					QueryMessage queryMsg = new QueryMessage(-1, query);
					client.query(queryMsg);
				} catch (ParseError e) {
					System.out.println(e.getErrors());
				}
				// client.query(line);
			}
		}

		log.info("Test ended");
		client.stop();
		for (Slave s : slaves) {
			s.stop();
		}
		coord.stop();
	}
}

package scratch;

import java.io.File;

import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.query.parser.QueryParser;
import de.tuberlin.dima.presslufthammer.query.parser.QueryParser.ParseError;
import de.tuberlin.dima.presslufthammer.transport.CLIClient;
import de.tuberlin.dima.presslufthammer.transport.Coordinator;
import de.tuberlin.dima.presslufthammer.transport.Leaf;
import de.tuberlin.dima.presslufthammer.transport.messages.QueryMessage;

/**
 * Test the query handling by creating a coordinator and some leafs in the same
 * process.
 * 
 * @author Aljoscha Krettek
 */
public class QueryTest {
    private static final String HOST = "localhost";
    private static final int PORT = 44444;
    private static final String DATASOURCES = "src/main/example-data/DataSources.xml";
    private static final File LEAF_DATADIR = new File("data-dir");

    public static void main(String[] args) throws Exception {

        Coordinator coord = new Coordinator(PORT, DATASOURCES);
        coord.start();
        Leaf leaf1 = new Leaf(HOST, PORT, LEAF_DATADIR);
        Leaf leaf2 = new Leaf(HOST, PORT, LEAF_DATADIR);
        Leaf leaf3 = new Leaf(HOST, PORT, LEAF_DATADIR);
        leaf1.start();
        leaf2.start();
        leaf3.start();

        CLIClient client = new CLIClient(HOST, PORT);

        if (client.start()) {
            try {
                Query query = QueryParser
                        .parse("SELECT * FROM Document WHERE Document.Name.Url==\"http://A\"");
                System.out.println("QUERY: " + query);
                QueryMessage queryMsg = new QueryMessage(-1, query);
                client.query(queryMsg);
            } catch (ParseError e) {
                System.out.println(e.getErrors());
            }
        }

        Thread.sleep(2000);

        client.stop();
        leaf1.stop();
        leaf2.stop();
        leaf3.stop();
        coord.stop();
    }
}

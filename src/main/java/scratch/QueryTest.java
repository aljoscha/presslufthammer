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
    private static final String CONFIG = "src/main/example-data/config.json";
    private static final File LEAF_DATADIR = new File("data-dir");

    public static void main(String[] args) throws Exception {

        Coordinator coord = new Coordinator(PORT, CONFIG);
        coord.start();
        Leaf leaf1 = new Leaf(HOST, PORT, LEAF_DATADIR);
        Leaf leaf2 = new Leaf(HOST, PORT, LEAF_DATADIR);
        Leaf leaf3 = new Leaf(HOST, PORT, LEAF_DATADIR);
        leaf1.start();
        // leaf2.start();
        // leaf3.start();

        CLIClient client = new CLIClient(HOST, PORT);

        if (client.start()) {
            try {
                Query query = QueryParser
                // .parse("SELECT * FROM Document");
                // .parse("SELECT * FROM Document WHERE Document.Name.Language.Country == \"gb\"");
                // .parse("SELECT * FROM Sentence");
                // .parse("SELECT * FROM Sentence WHERE Sentence.predicate.arguments.role == \"PMOD\" OR Sentence.predicate.arguments.role == \"NMOD\"");
                // .parse("SELECT * FROM Sentence WHERE Sentence.predicate.arguments.role == \"ADV\" OR Sentence.predicate.arguments.role == \"DEP\"");
                // .parse("SELECT Document.DocId AS ID, COUNT(Document.Links.Forward) AS bla FROM Document");
                // .parse("SELECT COUNT(Sentence.predicate.text) FROM Sentence");
                // .parse("SELECT Sentence.predicate.text, COUNT(Sentence.predicate.arguments.text) FROM Sentence WHERE Sentence.predicate.arguments.role == \"ADV\"");
                        .parse("SELECT Sentence.predicate.text, COUNT(Sentence.predicate.arguments.text) FROM Sentence");
                System.out.println("QUERY: " + query);
                QueryMessage queryMsg = new QueryMessage(-1, query);
                client.query(queryMsg);
            } catch (ParseError e) {
                System.out.println(e.getErrors());
            }
        }

        Thread.sleep(1000);

        client.stop();
        leaf1.stop();
        leaf2.stop();
        leaf3.stop();
        coord.stop();
    }
}

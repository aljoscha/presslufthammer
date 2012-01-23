package scratch;

import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class ProtobufTest {

    public static void main(String[] args) {
        SchemaNode schema = ProtobufSchemaHelper
                .readSchemaFromFile("src/main/example-data/Document.proto");

        System.out.println(schema);
    }
}

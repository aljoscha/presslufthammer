package scratch;

import java.io.FileNotFoundException;
import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.FieldStriper;
import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryWriteonlyTablet;
import de.tuberlin.dima.presslufthammer.data.hierarchical.json.JSONRecordFile;

public class JSONDefinitionTest {

    public static void main(String[] args) throws FileNotFoundException,
            IOException {

        SchemaNode schema = ProtobufSchemaHelper
                .readSchemaFromFile("src/main/example-data/Document.proto");

        System.out.println(schema.toString());
        JSONRecordFile records = new JSONRecordFile(schema,
                "src/main/example-data/documents.json");
        InMemoryWriteonlyTablet dummyTablet = new InMemoryWriteonlyTablet(
                schema);
        FieldStriper striper = new FieldStriper(schema);
        striper.dissectRecords(records, dummyTablet);

        dummyTablet.printColumns();
    }
}
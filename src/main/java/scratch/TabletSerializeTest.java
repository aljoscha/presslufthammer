package scratch;

import java.io.FileNotFoundException;
import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.AssemblyFSM;
import de.tuberlin.dima.presslufthammer.data.FieldStriper;
import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryReadonlyTablet;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryWriteonlyTablet;
import de.tuberlin.dima.presslufthammer.data.hierarchical.json.JSONRecordFile;

public class TabletSerializeTest {

    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        SchemaNode schema = ProtobufSchemaHelper
                .readSchemaFromFile("src/main/example-data/Document.proto");

        System.out.println(schema.toString());
        JSONRecordFile records = new JSONRecordFile(schema,
                "src/main/example-data/documents.json");

        InMemoryWriteonlyTablet inMemoryTablet = new InMemoryWriteonlyTablet(
                schema);
        FieldStriper striper = new FieldStriper(schema);
        striper.dissectRecords(records.recordIterator(), inMemoryTablet, -1);

        inMemoryTablet.printColumns();

        byte[] tabletData = inMemoryTablet.serialize();

        InMemoryReadonlyTablet tablet = new InMemoryReadonlyTablet(tabletData);

        AssemblyFSM fsm = new AssemblyFSM(schema);
        System.out.println(fsm.toString());

        JSONRecordFile outRecords = new JSONRecordFile(schema,
                "documents-out.json");

        fsm.assembleRecords(tablet, outRecords);
    }

}

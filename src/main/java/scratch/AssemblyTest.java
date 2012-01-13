package scratch;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.common.io.Resources;

import de.tuberlin.dima.presslufthammer.data.AssemblyFSM;
import de.tuberlin.dima.presslufthammer.data.FieldStriper;
import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryTablet;
import de.tuberlin.dima.presslufthammer.data.hierarchical.json.JSONRecordFile;

public class AssemblyTest {

    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        SchemaNode schema = ProtobufSchemaHelper.readSchema(Resources
                .getResource("Document.proto").getFile(), "Document");

        System.out.println(schema.toString());
        JSONRecordFile records = new JSONRecordFile(schema, Resources
                .getResource("documents.json").getFile());

        InMemoryTablet inMemoryTablet = new InMemoryTablet(schema);
        FieldStriper striper = new FieldStriper(schema);
        striper.dissectRecords(records, inMemoryTablet);

        inMemoryTablet.printColumns();

        AssemblyFSM fsm = new AssemblyFSM(schema);
        System.out.println(fsm.toString());

        JSONRecordFile outRecords = new JSONRecordFile(schema,
                "documents-out.json");

        fsm.assembleRecords(inMemoryTablet, outRecords);
    }

}

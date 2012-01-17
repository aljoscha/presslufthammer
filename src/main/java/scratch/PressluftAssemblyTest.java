package scratch;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.common.io.Resources;

import de.tuberlin.dima.presslufthammer.data.AssemblyFSM;
import de.tuberlin.dima.presslufthammer.data.FieldStriper;
import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.InMemoryReadonlyTablet;
import de.tuberlin.dima.presslufthammer.data.columnar.InMemoryWriteonlyTablet;
import de.tuberlin.dima.presslufthammer.data.hierarchical.json.JSONRecordFile;
import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;

public class PressluftAssemblyTest {

    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        SchemaNode schema = ProtobufSchemaHelper.readSchemaFromFile(Resources
                .getResource("Document.proto").getFile());

        System.out.println(schema.toString());
        JSONRecordFile records = new JSONRecordFile(schema, Resources
                .getResource("documents.json").getFile());

        InMemoryWriteonlyTablet inMemoryTablet = new InMemoryWriteonlyTablet(schema);
        FieldStriper striper = new FieldStriper(schema);
        striper.dissectRecords(records, inMemoryTablet);

        inMemoryTablet.printColumns();
        
        Pressluft resultTablet = inMemoryTablet.toPressluft();
        
        InMemoryReadonlyTablet tablet = new InMemoryReadonlyTablet(resultTablet);

        AssemblyFSM fsm = new AssemblyFSM(schema);
        System.out.println(fsm.toString());

        JSONRecordFile outRecords = new JSONRecordFile(schema,
                "documents-out.json");

        fsm.assembleRecords(tablet, outRecords);
    }

}

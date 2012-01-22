package scratch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.io.Resources;

import de.tuberlin.dima.presslufthammer.data.AssemblyFSM;
import de.tuberlin.dima.presslufthammer.data.FieldStriper;
import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryReadonlyTablet;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryWriteonlyTablet;
import de.tuberlin.dima.presslufthammer.data.hierarchical.json.JSONRecordFile;

public class ProjectionTest {

    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        SchemaNode schema = ProtobufSchemaHelper.readSchemaFromFile(Resources
                .getResource("Document.proto").getFile());

        System.out.println("ORIGINAL SCHEMA:");
        System.out.println(schema);
        System.out.println("-------------------------");

        Set<String> projectedFields = Sets.newHashSet("Document.DocId",
                "Document.Name.Language.Code", "Document.Name.Language.Country");
        SchemaNode projectedSchema = schema.project(projectedFields);

        System.out.println("PROJECTED SCHEMA:");
        System.out.println(projectedSchema);
        System.out.println("-------------------------");
        
        
        JSONRecordFile records = new JSONRecordFile(schema, Resources
                .getResource("documents.json").getFile());

        InMemoryWriteonlyTablet writeTablet = new InMemoryWriteonlyTablet(schema);
        FieldStriper striper = new FieldStriper(schema);
        striper.dissectRecords(records, writeTablet);

        writeTablet.printColumns();
        
        InMemoryReadonlyTablet readTablet = new InMemoryReadonlyTablet(writeTablet);

        AssemblyFSM fsm = new AssemblyFSM(projectedSchema);
        System.out.println(fsm.toString());

        JSONRecordFile outRecords = new JSONRecordFile(projectedSchema,
                "documents-out.json");

        fsm.assembleRecords(readTablet, outRecords);

    }

}

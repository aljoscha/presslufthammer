package scratch;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.common.io.Resources;

import de.tuberlin.dima.presslufthammer.data.FieldStriper;
import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryTablet;
import de.tuberlin.dima.presslufthammer.data.hierarchical.json.JSONRecordFile;

public class JSONDefinitionTest {

    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        
        SchemaNode schema = ProtobufSchemaHelper.readSchema(Resources
                .getResource("Document.proto").getFile(), "Document");

        System.out.println(schema.toString());
        JSONRecordFile records = new JSONRecordFile(schema,
                Resources.getResource("documents.json").getFile());
        InMemoryTablet dummyTablet = new InMemoryTablet(schema);
        FieldStriper striper = new FieldStriper(schema, dummyTablet);
        striper.dissectRecords(records);

        dummyTablet.printColumns();
    }
}

package scratch;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.common.io.Resources;

import de.tuberlin.dima.presslufthammer.data.FieldStriper;
import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.RecordProvider;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.dummy.DummyTablet;
import de.tuberlin.dima.presslufthammer.data.json.JSONRecordProvider;

public class JSONDefinitionTest {

    public static void main(String[] args) throws FileNotFoundException,
            IOException {

        
        SchemaNode schema = ProtobufSchemaHelper.readSchema(Resources
                .getResource("Document.proto").getFile(), "Document");

        System.out.println(schema.toString());
        RecordProvider recordProvider = new JSONRecordProvider(schema,
                Resources.getResource("documents.json"));
        DummyTablet dummyTablet = new DummyTablet();
        FieldStriper striper = new FieldStriper(schema, dummyTablet);
        striper.dissectRecords(recordProvider);

        dummyTablet.printColumns();
    }
}

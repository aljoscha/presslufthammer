package scratch;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.common.io.Resources;

import de.tuberlin.dima.presslufthammer.data.AssemblyFSM;
import de.tuberlin.dima.presslufthammer.data.FieldStriper;
import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ondisk.OnDiskTablet;
import de.tuberlin.dima.presslufthammer.data.hierarchical.json.JSONRecordFile;

public class OnDiskAssemblyTest {

    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        SchemaNode schema = ProtobufSchemaHelper.readSchema(Resources
                .getResource("Document.proto").getFile(), "Document");
        System.out.println(schema.toString());
        
        JSONRecordFile records = new JSONRecordFile(schema, Resources
                .getResource("documents.json").getFile());

        OnDiskTablet.removeTablet(new File("documents.tablet"));
        OnDiskTablet tablet = OnDiskTablet.createTablet(schema, new File("documents.tablet"));
        FieldStriper striper = new FieldStriper(schema);
        striper.dissectRecords(records, tablet);
        tablet.flush();

        AssemblyFSM fsm = new AssemblyFSM(schema);
//        System.out.println(fsm.toString());

        (new File("documents-out.json")).delete();
        JSONRecordFile outRecords = new JSONRecordFile(schema,
                "documents-out.json");

        fsm.assembleRecords(tablet, outRecords);
        tablet.close();
        
        
        
        // Now reopen the tablet ...
        
        OnDiskTablet reopenedTablet = OnDiskTablet.openTablet(new File("documents.tablet"), "Document");
        SchemaNode reopenedSchema = reopenedTablet.getSchema();
        
        AssemblyFSM reopenedFsm = new AssemblyFSM(reopenedSchema);
//        System.out.println(fsm.toString());

        (new File("documents-out2.json")).delete();
        JSONRecordFile outRecords2 = new JSONRecordFile(reopenedSchema,
                "documents-out2.json");

        reopenedFsm.assembleRecords(reopenedTablet, outRecords2);
        reopenedTablet.close();
        
    }

}

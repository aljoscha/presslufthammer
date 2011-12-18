package scratch;

import com.google.common.io.Resources;

import de.tuberlin.dima.presslufthammer.data.AssemblyFSM;
import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryTablet;

public class AssemblyTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
        SchemaNode schema = ProtobufSchemaHelper.readSchema(Resources
                .getResource("Document.proto").getFile(), "Document");

        Tablet tablet = new InMemoryTablet(schema);

        AssemblyFSM fsm = new AssemblyFSM(schema);
        System.out.println(fsm.toString());
    }

}

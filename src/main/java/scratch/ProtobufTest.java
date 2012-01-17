package scratch;

import com.google.common.io.Resources;

import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class ProtobufTest {

    public static void main(String[] args) {
        SchemaNode schema = ProtobufSchemaHelper.readSchemaFromFile(Resources
                .getResource("Document.proto").getFile());
        
        System.out.println(schema);
    }
}

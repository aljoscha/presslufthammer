package scratch;

import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class SchemaTest {

    /**
     * @param args
     */
    public static void main(String[] args) {

        SchemaNode root = SchemaNode.createRecord("predicate");
        root.addField(SchemaNode.createPrimitive("lemma", PrimitiveType.STRING));
        root.addField(SchemaNode.createPrimitive("text", PrimitiveType.STRING));

        SchemaNode arguments = SchemaNode.createRecord("arguments");
        arguments.addField(SchemaNode.createPrimitive("text",
                PrimitiveType.STRING));
        arguments.addField(SchemaNode.createPrimitive("role",
                PrimitiveType.STRING));
        arguments.setRepeated();
        root.addField(arguments);

        System.out.println(root.toString());
    }

}

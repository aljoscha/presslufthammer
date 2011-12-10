package scratch;

import java.io.FileNotFoundException;
import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.ColumnStriper;
import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.RecordProvider;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.dummy.DummyFieldWriterFactory;
import de.tuberlin.dima.presslufthammer.data.json.JSONRecordProvider;

public class JSONReaderTest {

    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        SchemaNode predicate = SchemaNode.createRecord("predicate");
        predicate.addField(SchemaNode.createPrimitive("lemma",
                PrimitiveType.STRING));
        predicate.addField(SchemaNode.createPrimitive("text",
                PrimitiveType.STRING));

        SchemaNode arguments = SchemaNode.createRecord("arguments");
        arguments.addField(SchemaNode.createPrimitive("text",
                PrimitiveType.STRING));
        arguments.addField(SchemaNode.createPrimitive("role",
                PrimitiveType.STRING));
        arguments.setRepeated();
        predicate.addField(arguments);
        
        SchemaNode penises = SchemaNode.createPrimitive("numbers", PrimitiveType.LONG);
        penises.setRepeated();

        SchemaNode schemaRoot = SchemaNode.createRecord("PredicateOuter");
        schemaRoot.addField(penises);
        schemaRoot.addField(predicate);

        System.out.println(predicate.toString());
        RecordProvider recordProvider = new JSONRecordProvider(schemaRoot,
                "sentences-reducedPunctuation-json-1-2");
        ColumnStriper striper = new ColumnStriper(schemaRoot,
                new DummyFieldWriterFactory());
        striper.dissectRecords(recordProvider);
    }
}

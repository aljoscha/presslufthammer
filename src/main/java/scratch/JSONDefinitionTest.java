package scratch;

import java.io.FileNotFoundException;
import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.ColumnStriper;
import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.RecordProvider;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.dummy.DummyFieldWriterFactory;
import de.tuberlin.dima.presslufthammer.data.json.JSONRecordProvider;

public class JSONDefinitionTest {

    public static void main(String[] args) throws FileNotFoundException,
            IOException {
        
        SchemaNode document = SchemaNode.createRecord("Document");
        SchemaNode docId = SchemaNode.createPrimitive("DocId", PrimitiveType.LONG);
        document.addField(docId);
        
        SchemaNode links = SchemaNode.createRecord("Links");
        links.setOptional();
        SchemaNode backward = SchemaNode.createPrimitive("Backward", PrimitiveType.LONG);
        backward.setRepeated();
        SchemaNode forward = SchemaNode.createPrimitive("Forward", PrimitiveType.LONG);
        forward.setRepeated();
        links.addField(backward);
        links.addField(forward);
        document.addField(links);
        
        SchemaNode name = SchemaNode.createRecord("Name");
        name.setRepeated();
        SchemaNode language = SchemaNode.createRecord("Language");
        language.setRepeated();
        SchemaNode code = SchemaNode.createPrimitive("Code", PrimitiveType.STRING);
        SchemaNode country = SchemaNode.createPrimitive("Country", PrimitiveType.STRING);
        country.setOptional();
        language.addField(code);
        language.addField(country);
        name.addField(language);
        SchemaNode url = SchemaNode.createPrimitive("Url", PrimitiveType.STRING);
        url.setOptional();
        name.addField(url);
        
        document.addField(name);
        
        

        System.out.println(document.toString());
        RecordProvider recordProvider = new JSONRecordProvider(document,
                "documents.json");
        ColumnStriper striper = new ColumnStriper(document,
                new DummyFieldWriterFactory());
        striper.dissectRecords(recordProvider);
    }
}

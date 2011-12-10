package de.tuberlin.dima.presslufthammer.data.dummy;

import de.tuberlin.dima.presslufthammer.data.FieldWriter;
import de.tuberlin.dima.presslufthammer.data.FieldWriterFactory;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class DummyFieldWriterFactory implements FieldWriterFactory {
    public FieldWriter createFieldWriter(FieldWriter parent, SchemaNode schema) {
        return new DummyFieldWriter(parent, schema);
    }
}

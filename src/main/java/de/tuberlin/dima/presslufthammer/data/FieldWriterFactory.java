package de.tuberlin.dima.presslufthammer.data;

public interface FieldWriterFactory {
    public FieldWriter createFieldWriter(FieldWriter parent, SchemaNode schema);
}

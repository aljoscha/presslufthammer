package de.tuberlin.dima.presslufthammer.data;

public interface RecordDecoder {
    public RecordDecoder newDecoder(SchemaNode schema, Object data);

    public FieldIterator fieldIterator();
}

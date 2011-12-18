package de.tuberlin.dima.presslufthammer.data.hierarchical;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public interface RecordDecoder {
    public RecordDecoder newDecoder(SchemaNode schema, Object data);

    public FieldIterator fieldIterator();
}

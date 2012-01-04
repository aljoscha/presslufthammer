package de.tuberlin.dima.presslufthammer.data.hierarchical;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public interface RecordEncoder {
    public void appendValue(SchemaNode schema, Object value);

    public void moveToLevel(SchemaNode schema);

    public void returnToLevel(SchemaNode schema);

    public void finalizeRecord();
}
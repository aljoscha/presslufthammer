package de.tuberlin.dima.presslufthammer.data.columnar;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public interface Tablet {
    public SchemaNode getSchema();
    public ColumnWriter getColumnWriter(SchemaNode schema);
}

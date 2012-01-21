package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public interface DataStore {
    public Tablet getTablet(String tableName, int partition) throws IOException;

    public boolean hasTablet(String tableName, int partition);

    public Tablet createOrGetTablet(SchemaNode schema, int partition)
            throws IOException;
}

package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public interface DataStore {
    public Tablet getTablet(SchemaNode schema, int partition)
            throws IOException;
}

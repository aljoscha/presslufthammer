package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

/**
 * Interface representing a datastore, a datastore contains tablets for the
 * partitions of a table.
 * 
 * @author Aljoscha Krettek
 * 
 */
public interface DataStore {
    /**
     * Returns the {@link Tablet} with columnar data of the specified partition
     * of the specified table. This method will return null if the tablet is not
     * available.
     */
    public Tablet getTablet(String tableName, int partition) throws IOException;

    /**
     * Returns true if the datastore has a {@link Tablet} for the specified
     * table/partition.
     */
    public boolean hasTablet(String tableName, int partition);

    /**
     * Returns the {@link Tablet} with columnar data of the specified partition
     * of the specified table. This method will create a new tablet when the
     * datastore does not have a tablt for the specified table/partition.
     */
    public Tablet createOrGetTablet(SchemaNode schema, int partition)
            throws IOException;
}

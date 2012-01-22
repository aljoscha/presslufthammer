package de.tuberlin.dima.presslufthammer.data.columnar;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

/**
 * Interface for working with a tablet. A tablet has an associated schema and
 * manages columnar data. The data can be read and written using column
 * readers/writers retrieved from the tablet.
 * 
 * @author Aljoscha Krettek
 * 
 */
public interface Tablet {
    /**
     * Returns the schema associated with this tablet.
     */
    public SchemaNode getSchema();

    /**
     * Returns true when this tablet contains a column for the given schema.
     */
    public boolean hasColumn(SchemaNode schema);

    /**
     * Returns a {@link ColumnWriter} for writing data to the column specified
     * by the given schema.
     */
    public ColumnWriter getColumnWriter(SchemaNode schema);

    /**
     * Returns a {@link ColumnReader} for reading data from column specified by
     * the given schema.
     */
    public ColumnReader getColumnReader(SchemaNode schema);
}

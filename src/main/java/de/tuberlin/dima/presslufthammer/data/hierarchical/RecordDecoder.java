package de.tuberlin.dima.presslufthammer.data.hierarchical;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

/**
 * A decoder for one record of a hierarchical data format. Implementations must
 * be provided for every supported hierarchical data format.
 * 
 * <p>
 * This is used by {@link FieldStriper} during striping of records to columnar
 * data ({@link Tablet} and {@link AssemblyASM}).
 * 
 * @author Aljoscha Krettek
 * 
 */
public interface RecordDecoder {
    /**
     * Create a new decoder of the underlying implementation for the given
     * schema and the given data.
     */
    public RecordDecoder newDecoder(SchemaNode schema, Object data);

    /**
     * Return a {@link FieldIterator} for the fields of the record. This returns
     * an iterator specific to the underlying hierarchical data format.
     */
    public FieldIterator fieldIterator();
}

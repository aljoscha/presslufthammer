package de.tuberlin.dima.presslufthammer.data.hierarchical;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

/**
 * An interface that must be used to create hierarchical records. An
 * implementation must be provided for every underlying data format.
 * 
 * <p>
 * Objects supporting this interface are returned from {@link RecordStore} for a
 * specific underlying data format.
 * 
 * <p>
 * This is used by {@link AssemblyASM} while creating hierarchical records from
 * columnar data.
 * 
 * <p>
 * See the dremel paper for details on the {@code moveToLevel} and
 * {@code returnToLevel} methods. The actual workings of these methods, the
 * "opening" and "closing" of "tags" depends on the actual implementation for
 * the hierarchical data format.
 * 
 * @author Aljoscha Krettek
 * 
 */
public interface RecordEncoder {
    /**
     * Appends the given value to the record for the field specified by the
     * given schema.
     */
    public void appendValue(SchemaNode schema, Object value);

    /**
     * Creates opening "tags" (record fields) up to the level specified by the
     * schema.
     */
    public void moveToLevel(SchemaNode schema);

    /**
     * Creates closing "tags" (record fields) back to the level specified by the
     * schema.
     */
    public void returnToLevel(SchemaNode schema);

    /**
     * Create closing "tags" for all nested records fields that are still
     * "open".
     */
    public void finalizeRecord();
}
package de.tuberlin.dima.presslufthammer.data.hierarchical;

/**
 * An iterator that returns field from a hierarchical record. Objects
 * implementing this interface are returned by {@link RecordDecoder}. An
 * implementation must be provided for every supported hierarchical data format.
 * 
 * @author Aljoscha Krettek
 * 
 */
public interface FieldIterator {
    /**
     * Returns the next field from the record.
     */
    public Field next();
}

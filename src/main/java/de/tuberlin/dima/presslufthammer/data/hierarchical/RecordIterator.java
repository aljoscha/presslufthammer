package de.tuberlin.dima.presslufthammer.data.hierarchical;

/**
 * An iterator that returns {@link RecordDecoders} for reading files from a
 * {@link RecordStore}. Implementation for every supported hierarchical data
 * format.
 * 
 * @author Aljoscha Krettek
 * 
 */
public interface RecordIterator {
    /**
     * Returns a record decoder for the next hierarchical record.
     */
    public RecordDecoder next();
}

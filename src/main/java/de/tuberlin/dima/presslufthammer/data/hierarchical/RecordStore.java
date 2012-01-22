package de.tuberlin.dima.presslufthammer.data.hierarchical;

/**
 * Interface that represents a store of hierarchical records. Provides methods
 * to get an iterator to iterate over the stored records and a method to create
 * new records.
 * 
 * @author Aljoscha Krettek
 * 
 */
public interface RecordStore {
    /**
     * Returns an iterator that iterates over all records in this record store.
     */
    public RecordIterator recordIterator();

    /**
     * Starts a new record and returns a {@link RecordEncoder} for writing data
     * to that record.
     */
    public RecordEncoder startRecord();
}

package de.tuberlin.dima.presslufthammer.data.hierarchical;

public interface RecordStore {
    public RecordIterator recordIterator();
    public RecordEncoder startRecord();
}

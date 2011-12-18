package de.tuberlin.dima.presslufthammer.data.hierarchical.json;

import java.io.FileNotFoundException;
import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordIterator;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordStore;

public class JSONRecordFile implements RecordStore {
    private SchemaNode schema;
    private String filename;

    public JSONRecordFile(SchemaNode schema, String filename)
            throws FileNotFoundException, IOException {
        this.schema = schema;
        this.filename = filename;
    }

    @Override
    public RecordIterator recordIterator() {
        return new JSONRecordFileIterator(schema, filename);
    }

}

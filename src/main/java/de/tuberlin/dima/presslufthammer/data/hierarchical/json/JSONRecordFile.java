package de.tuberlin.dima.presslufthammer.data.hierarchical.json;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordEncoder;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordIterator;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordStore;

public class JSONRecordFile implements RecordStore {
    private SchemaNode schema;
    private String filename;

    public JSONRecordFile(SchemaNode schema, String filename) {
        this.schema = schema;
        this.filename = filename;
    }

    @Override
    public RecordIterator recordIterator() {
        return new JSONRecordFileIterator(schema, filename);
    }

    public RecordEncoder startRecord() {
        return new JSONRecordEncoder(schema, this);
    }

    public void writeRecord(JSONRecordEncoder record) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filename,
                true));
        writer.write(record.getJob().toJSONString());
        writer.write("\n");
        writer.close();
    }
}

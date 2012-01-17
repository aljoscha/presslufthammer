package de.tuberlin.dima.presslufthammer.data.hierarchical.json;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordIterator;

public class JSONRecordPrinter extends JSONRecordFile {

    public JSONRecordPrinter(SchemaNode schema) {
        super(schema, null);
    }

    @Override
    public RecordIterator recordIterator() {
        return null;
    }

    @Override
    public void writeRecord(JSONRecordEncoder record) throws IOException {
        System.out.println(record.getJob().toJSONString());
    }
}

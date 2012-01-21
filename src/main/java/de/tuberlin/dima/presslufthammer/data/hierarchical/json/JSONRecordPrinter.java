package de.tuberlin.dima.presslufthammer.data.hierarchical.json;

import java.io.IOException;
import java.io.PrintWriter;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordIterator;

public class JSONRecordPrinter extends JSONRecordFile {
    private PrintWriter out;
    
    public JSONRecordPrinter(SchemaNode schema, PrintWriter out) {
        super(schema, null);
        this.out = out;
    }

    @Override
    public RecordIterator recordIterator() {
        return null;
    }

    @Override
    public void writeRecord(JSONRecordEncoder record) throws IOException {
        out.println(record.getJob().toJSONString());
    }
}

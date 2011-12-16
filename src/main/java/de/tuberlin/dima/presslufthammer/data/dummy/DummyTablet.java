package de.tuberlin.dima.presslufthammer.data.dummy;

import java.util.LinkedList;
import java.util.List;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;

public class DummyTablet implements Tablet {
    private List<DummyColumnWriter> columnWriters = new LinkedList<DummyColumnWriter>();

    public ColumnWriter getColumnWriter(SchemaNode schema) {
        DummyColumnWriter writer = new DummyColumnWriter(schema);
        columnWriters.add(writer);
        return writer;
    }

    public void printColumns() {
        for (DummyColumnWriter writer : columnWriters) {
            writer.printToStdout();
        }
    }
}

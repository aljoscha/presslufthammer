package de.tuberlin.dima.presslufthammer.qexec.grouping;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

public class Int64CountGroupField extends GroupField {
    long value = 0;
    int repetition = -1;
    int definition = -1;

    public Int64CountGroupField(ColumnWriter writer) {
        super(writer);
        // evil hack, definition is only ever checked for
        // being to small, so large numbers do not matter
        definition = Integer.MAX_VALUE;
    }

    @Override
    public void addValue(ColumnReader reader) {
        if (reader.isNull()) {
            // ignore
        } else {
            value += 1;
        }
        if (repetition == -1) {
            repetition = reader.getWriteRepetition();
        }
        if (reader.getCurrentDefinition() > definition) {
            definition = reader.getCurrentDefinition();
        }
    }

    @Override
    public void emit() throws IOException {
        if (repetition == -1) {
            return;
        }
        writer.writeInt64(value, repetition, definition);
    }

}

package de.tuberlin.dima.presslufthammer.qexec.grouping;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

public class Int64PlainGroupField extends GroupField {
    private long value;
    private int repetition = -1;
    private int definition = -1;

    public Int64PlainGroupField(ColumnWriter writer) {
        super(writer);
    }

    @Override
    public void addValue(ColumnReader reader) {
        if (repetition >= 0) {
            // no need for another value
            return;
        }
        if (reader.isNull()) {
            return;
        }
        value = reader.getInt64();
        repetition = reader.getCurrentRepetition();
        definition = reader.getCurrentDefinition();
    }

    @Override
    public void emit() throws IOException {
        if (repetition == -1) {
            writer.writeNull(repetition, definition);
        } else {
            writer.writeInt64(value, repetition, definition);
        }
    }

}

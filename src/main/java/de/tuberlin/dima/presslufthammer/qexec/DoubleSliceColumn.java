package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;

import com.google.common.base.Preconditions;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

public class DoubleSliceColumn extends SliceColumn {
    private double currentValue = -1;

    public DoubleSliceColumn(ColumnReader reader) {
        super(reader);
    }

    @Override
    public void advance() throws IOException {
        currentRep = reader.getNextRepetition();
        currentDef = reader.getNextDefinition();
        currentValue = reader.getNextDouble();
    }

    @Override
    public void writeValue(ColumnWriter writer) throws IOException {
        Preconditions.checkState(currentRep >= 0,
                "Slice does not yet have value.");
        writer.writeDouble(currentValue, currentRep, currentDef);
    }

    @Override
    public double getDouble() {
        return currentValue;
    }
}

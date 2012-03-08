package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;

import com.google.common.base.Preconditions;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

public class BoolSliceColumn extends SliceColumn {
    private boolean currentValue = false;

    public BoolSliceColumn(ColumnReader reader) {
        super(reader);
    }

    @Override
    public void advance() throws IOException {
        currentRep = reader.getNextRepetition();
        currentDef = reader.getNextDefinition();
        currentValue = reader.getNextBool();
    }

    @Override
    public void writeValue(ColumnWriter writer) throws IOException {
        Preconditions.checkState(currentRep >= 0,
                "Slice does not yet have value.");
        writer.writeBool(currentValue, currentRep, currentDef);
    }

    @Override
    public boolean getBool() {
        return currentValue;
    }
}

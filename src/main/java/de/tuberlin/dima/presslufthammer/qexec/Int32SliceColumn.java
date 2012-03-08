package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;

import com.google.common.base.Preconditions;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

public class Int32SliceColumn extends SliceColumn {
    private int currentValue = -1;
    private boolean currentIsNull = false;

    public Int32SliceColumn(ColumnReader reader) {
        super(reader);
    }

    @Override
    public void advance() throws IOException {
        currentRep = reader.getNextRepetition();
        currentDef = reader.getNextDefinition();
        if (reader.nextIsNull()) {
            currentIsNull = true;
        } else {
            currentValue = reader.getNextInt32();
        }
    }

    @Override
    public void writeValue(ColumnWriter writer) throws IOException {
        Preconditions.checkState(currentRep >= 0,
                "Slice does not yet have value.");
        if (currentIsNull) {
            writer.writeNull(currentRep, currentDef);
        } else {
            writer.writeInt32(currentValue, currentRep, currentDef);
        }
    }

    @Override
    public int getInt32() {
        return currentValue;
    }
}

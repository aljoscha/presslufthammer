package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;

import com.google.common.base.Preconditions;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

public class StringSliceColumn extends SliceColumn {
    private String currentValue = null;
    private boolean currentIsNull = false;

    public StringSliceColumn(ColumnReader reader) {
        super(reader);
    }

    @Override
    public void advance() throws IOException {
        currentRep = reader.getNextRepetition();
        currentDef = reader.getNextDefinition();
        if (reader.nextIsNull()) {
            currentIsNull = true;
            reader.getNextValue();
        } else {
            currentIsNull = false;
            currentValue = reader.getNextString();
        }
    }

    @Override
    public void writeValue(ColumnWriter writer) throws IOException {
        Preconditions.checkState(currentRep >= 0,
                "Slice does not yet have value.");
        if (currentIsNull) {
            writer.writeNull(currentRep, currentDef);
        } else {
            writer.writeString(currentValue, currentRep, currentDef);
        }
    }

    @Override
    public String getString() {
        return currentValue;
    }

    @Override
    public String asString() {
        return currentValue;
    }

    @Override
    public String toString() {
        if (currentIsNull) {

            return "rep: " + currentRep + " def: " + currentDef + " val: NULL";
        }
        return "rep: " + currentRep + " def: " + currentDef + " val: "
                + currentValue;
    }
}

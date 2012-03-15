package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

public abstract class SliceColumn {
    protected ColumnReader reader;
    protected int currentDef = -1;
    protected int currentRep = -1;

    public SliceColumn(ColumnReader reader) {
        this.reader = reader;
    }

    public boolean hasNext() throws IOException {
        return reader.hasNext();
    }

    public int getCurrentDefinition() {
        return currentDef;
    }

    public int getCurrentRepetition() {
        return currentRep;
    }

    public int getNextRepetition() throws IOException {
        return reader.getNextRepetition();
    }

    public abstract void advance() throws IOException;

    public abstract void writeValue(ColumnWriter writer) throws IOException;

    public int getInt32() {
        throw new RuntimeException("This is no int32 column slice.");
    }

    public long getInt64() {
        throw new RuntimeException("This is no int64 column slice.");
    }

    public float getFloat() {
        throw new RuntimeException("This is no float column slice.");
    }

    public double getDouble() {
        throw new RuntimeException("This is no double column slice.");
    }

    public boolean getBool() {
        throw new RuntimeException("This is no bool column slice.");
    }

    public String getString() {
        throw new RuntimeException("This is no String column slice.");
    }
    
    public String asString() {
        return "";
    }

    @Override
    public String toString() {
        return " ";
    }
}

package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.IOException;

public interface ColumnReader {
    public boolean hasNext() throws IOException;

    public boolean nextIsNull() throws IOException;

    public Object getNextValue() throws IOException;

    public int getNextRepetition() throws IOException;

    public int getNextDefinition() throws IOException;

    public int getNextInt32() throws IOException;

    public long getNextInt64() throws IOException;

    public boolean getNextBool() throws IOException;

    public float getNextFloat() throws IOException;

    public double getNextDouble() throws IOException;

    public String getNextString() throws IOException;
}

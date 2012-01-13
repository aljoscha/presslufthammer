package de.tuberlin.dima.presslufthammer.data.columnar;

public interface ColumnReader {
    public boolean hasNext();

    public boolean nextIsNull();

    public Object getNextValue();

    public int getNextRepetition();

    public int getNextDefinition();

    public int getNextInt32();

    public long getNextInt64();

    public boolean getNextBool();

    public float getNextFloat();

    public double getNextDouble();

    public String getNextString();
}

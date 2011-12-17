package de.tuberlin.dima.presslufthammer.data.columnar;

import de.tuberlin.dima.presslufthammer.data.Field;

public interface ColumnWriter {
    public void writeField(Field field, int repetitionLevel, int definitionLevel);
    public void writeInt32(int value, int repetitionLevel, int definitionLevel);
    public void writeInt64(long value, int repetitionLevel, int definitionLevel);
    public void writeBool(boolean value, int repetitionLevel, int definitionLevel);
    public void writeFloat(float value, int repetitionLevel, int definitionLevel);
    public void writeDouble(double value, int repetitionLevel, int definitionLevel);
    public void writeString(String value, int repetitionLevel, int definitionLevel);
    public void writeNull(int repetitionLevel, int definitionLevel);
}

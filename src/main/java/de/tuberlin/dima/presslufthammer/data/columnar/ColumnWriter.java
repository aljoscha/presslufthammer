package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.hierarchical.Field;

public interface ColumnWriter {
    public void writeField(Field field, int repetitionLevel, int definitionLevel) throws IOException;
    public void writeInt32(int value, int repetitionLevel, int definitionLevel) throws IOException;
    public void writeInt64(long value, int repetitionLevel, int definitionLevel) throws IOException;
    public void writeBool(boolean value, int repetitionLevel, int definitionLevel) throws IOException;
    public void writeFloat(float value, int repetitionLevel, int definitionLevel) throws IOException;
    public void writeDouble(double value, int repetitionLevel, int definitionLevel) throws IOException;
    public void writeString(String value, int repetitionLevel, int definitionLevel) throws IOException;
    public void writeNull(int repetitionLevel, int definitionLevel) throws IOException;
}

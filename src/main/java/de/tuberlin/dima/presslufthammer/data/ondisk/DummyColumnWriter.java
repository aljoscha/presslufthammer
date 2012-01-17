package de.tuberlin.dima.presslufthammer.data.ondisk;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.hierarchical.Field;

public class DummyColumnWriter implements ColumnWriter {

    protected DummyColumnWriter() {
    }

    public void writeField(Field field, int repetitionLevel, int definitionLevel) {
    }

    @Override
    public void writeInt32(int value, int repetitionLevel, int definitionLevel) {
    }

    @Override
    public void writeInt64(long value, int repetitionLevel, int definitionLevel) {
    }

    @Override
    public void writeBool(boolean value, int repetitionLevel,
            int definitionLevel) {
    }

    @Override
    public void writeFloat(float value, int repetitionLevel, int definitionLevel) {
    }

    @Override
    public void writeDouble(double value, int repetitionLevel,
            int definitionLevel) {
    }

    @Override
    public void writeString(String value, int repetitionLevel,
            int definitionLevel) {
    }
    
    @Override
    public void writeNull(int repetitionLevel,
            int definitionLevel) {
    }
}

package de.tuberlin.dima.presslufthammer.data.columnar;

import de.tuberlin.dima.presslufthammer.data.Field;

public interface ColumnWriter {
    public void writeField(Field field, int repetitionLevel, int definitionLevel);
}

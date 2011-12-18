package de.tuberlin.dima.presslufthammer.data.hierarchical.fields;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

public class BooleanField extends PrimitiveField {
    private final boolean value;

    public BooleanField(SchemaNode schema, boolean value) {
        super(schema);
        this.value = value;
    }

    @Override
    public String toString() {
        return Boolean.toString(value);
    }

    public boolean getValue() {
        return value;
    }

    @Override
    public void writeToColumn(ColumnWriter writer, int repetitionLevel,
            int definitionLevel) {
        writer.writeBool(value, repetitionLevel, definitionLevel);
    }
}

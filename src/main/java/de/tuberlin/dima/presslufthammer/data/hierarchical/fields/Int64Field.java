package de.tuberlin.dima.presslufthammer.data.hierarchical.fields;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

public class Int64Field extends PrimitiveField {
    private final long value;

    public Int64Field(SchemaNode schema, long value) {
        super(schema);
        this.value = value;
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    public long getValue() {
        return value;
    }

    @Override
    public void writeToColumn(ColumnWriter writer, int repetitionLevel,
            int definitionLevel) {
       writer.writeInt64(value, repetitionLevel, definitionLevel); 
    }
}

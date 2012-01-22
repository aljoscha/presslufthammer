package de.tuberlin.dima.presslufthammer.data.hierarchical.fields;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

/**
 * {@link Field} implementation for int32 values.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class Int32Field extends PrimitiveField {
    private final int value;

    public Int32Field(SchemaNode schema, int value) {
        super(schema);
        this.value = value;
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }

    public int getValue() {
        return value;
    }

    @Override
    public void writeToColumn(ColumnWriter writer, int repetitionLevel,
            int definitionLevel) throws IOException {
        writer.writeInt32(value, repetitionLevel, definitionLevel);
    }
}

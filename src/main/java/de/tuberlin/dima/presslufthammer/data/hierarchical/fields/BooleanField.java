package de.tuberlin.dima.presslufthammer.data.hierarchical.fields;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

/**
 * {@link Field} implementation for boolean values.
 * 
 * @author Aljoscha Krettek
 * 
 */
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
            int definitionLevel) throws IOException {
        writer.writeBool(value, repetitionLevel, definitionLevel);
    }
}

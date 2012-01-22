package de.tuberlin.dima.presslufthammer.data.hierarchical.fields;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

/**
 * {@link Field} implementation for string values.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class StringField extends PrimitiveField {
    private final String value;

    public StringField(SchemaNode schema, String value) {
        super(schema);
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public void writeToColumn(ColumnWriter writer, int repetitionLevel,
            int definitionLevel) throws IOException {
        writer.writeString(value, repetitionLevel, definitionLevel);
    }
}

package de.tuberlin.dima.presslufthammer.data.hierarchical.fields;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.hierarchical.Field;

/**
 * {@link Field} implementation for record fields. This provides an aditional
 * method to get the underlying raw data of the record field.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class RecordField extends Field {
    private final Object data;

    public RecordField(SchemaNode schema, Object data) {
        super(schema);
        this.data = data;
    }

    public Object getData() {
        return data;
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public String toString() {
        return "RECORD FIELD " + schema.getName() + ": " + data.toString();
    }

    @Override
    public void writeToColumn(ColumnWriter writer, int repetitionLevel,
            int definitionLevel) {
        // Do nothing for now ...
    }
}

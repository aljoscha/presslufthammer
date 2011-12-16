package de.tuberlin.dima.presslufthammer.data.fields;

import de.tuberlin.dima.presslufthammer.data.Field;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

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
}

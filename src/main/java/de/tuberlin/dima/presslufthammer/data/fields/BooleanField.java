package de.tuberlin.dima.presslufthammer.data.fields;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

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
}

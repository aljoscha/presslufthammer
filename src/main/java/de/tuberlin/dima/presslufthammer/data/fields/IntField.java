package de.tuberlin.dima.presslufthammer.data.fields;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class IntField extends PrimitiveField {
    private final int value;

    public IntField(SchemaNode schema, int value) {
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
}

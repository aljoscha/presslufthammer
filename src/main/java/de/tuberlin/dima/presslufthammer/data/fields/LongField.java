package de.tuberlin.dima.presslufthammer.data.fields;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class LongField extends PrimitiveField {
    private final long value;

    public LongField(SchemaNode schema, long value) {
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
}

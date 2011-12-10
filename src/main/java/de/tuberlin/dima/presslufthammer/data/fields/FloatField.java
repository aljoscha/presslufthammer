package de.tuberlin.dima.presslufthammer.data.fields;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class FloatField extends PrimitiveField {
    private final float value;

    public FloatField(SchemaNode schema, float value) {
        super(schema);
        this.value = value;
    }

    @Override
    public String toString() {
        return Float.toString(value);
    }

    public float getValue() {
        return value;
    }
}

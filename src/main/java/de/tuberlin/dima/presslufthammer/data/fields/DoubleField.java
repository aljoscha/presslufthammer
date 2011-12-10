package de.tuberlin.dima.presslufthammer.data.fields;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class DoubleField extends PrimitiveField {
    private final double value;

    public DoubleField(SchemaNode schema, double value) {
        super(schema);
        this.value = value;
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

    public double getValue() {
        return value;
    }
}

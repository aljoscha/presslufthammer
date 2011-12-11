package de.tuberlin.dima.presslufthammer.data.fields;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

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
}
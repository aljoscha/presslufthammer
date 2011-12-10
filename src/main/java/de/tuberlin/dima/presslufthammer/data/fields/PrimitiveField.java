package de.tuberlin.dima.presslufthammer.data.fields;

import de.tuberlin.dima.presslufthammer.data.Field;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public abstract class PrimitiveField extends Field {
    public PrimitiveField(SchemaNode schema) {
        super(schema);
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }
}

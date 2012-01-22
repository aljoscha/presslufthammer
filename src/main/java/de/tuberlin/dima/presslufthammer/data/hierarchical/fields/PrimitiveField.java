package de.tuberlin.dima.presslufthammer.data.hierarchical.fields;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.hierarchical.Field;

/**
 * Base class for all primitive type {@link Field} implementations.
 * 
 * @author Aljoscha Krettek
 * 
 */
public abstract class PrimitiveField extends Field {
    public PrimitiveField(SchemaNode schema) {
        super(schema);
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }
}

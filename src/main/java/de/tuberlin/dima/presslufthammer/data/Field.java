package de.tuberlin.dima.presslufthammer.data;

public abstract class Field {
    protected final SchemaNode schema;

    public Field(SchemaNode schema) {
        this.schema = schema;
    }

    public SchemaNode getSchema() {
        return schema;
    }

    public abstract boolean isPrimitive();
}

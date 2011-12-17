package de.tuberlin.dima.presslufthammer.data;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

public abstract class Field {
    protected final SchemaNode schema;

    public Field(SchemaNode schema) {
        this.schema = schema;
    }

    public SchemaNode getSchema() {
        return schema;
    }

    public abstract boolean isPrimitive();

    public abstract void writeToColumn(ColumnWriter writer,
            int repetitionLevel, int definitionLevel);
}

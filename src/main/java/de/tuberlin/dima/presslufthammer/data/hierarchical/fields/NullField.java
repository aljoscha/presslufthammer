package de.tuberlin.dima.presslufthammer.data.hierarchical.fields;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.hierarchical.Field;

public class NullField extends Field {

    public NullField(SchemaNode schema) {
        super(schema);
    }

    @Override
    public String toString() {
        return "NULL";
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public void writeToColumn(ColumnWriter writer, int repetitionLevel,
            int definitionLevel) {
        writer.writeNull(repetitionLevel, definitionLevel);
    }
}

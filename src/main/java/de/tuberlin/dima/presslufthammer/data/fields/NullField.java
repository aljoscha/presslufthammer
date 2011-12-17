package de.tuberlin.dima.presslufthammer.data.fields;

import de.tuberlin.dima.presslufthammer.data.Field;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

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

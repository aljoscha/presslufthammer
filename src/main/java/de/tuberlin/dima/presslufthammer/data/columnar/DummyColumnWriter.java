package de.tuberlin.dima.presslufthammer.data.columnar;

import java.util.List;

import com.google.common.collect.Lists;

import de.tuberlin.dima.presslufthammer.data.Field;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class DummyColumnWriter implements ColumnWriter {
    private SchemaNode schema;
    private List<Value> values;

    protected DummyColumnWriter(SchemaNode schema) {
        values = Lists.newLinkedList();
        this.schema = schema;
    }

    public void writeField(Field field, int repetitionLevel, int definitionLevel) {
        Value value = new Value(field, repetitionLevel, definitionLevel);
        values.add(value);

    }

    public void printToStdout() {
        System.out.println("COLUMN: " + schema.getQualifiedName());
        for (Value value : values) {
            System.out.println("r: " + value.repetitionLevel + ", d: "
                    + value.definitionLevel + ", field: " + value.field);
        }
    }

    private class Value {
        int repetitionLevel;
        int definitionLevel;
        Field field;

        public Value(Field field, int repetiotionLevel, int definitionLevel) {
            this.repetitionLevel = repetiotionLevel;
            this.definitionLevel = definitionLevel;
            this.field = field;
        }
    }
}

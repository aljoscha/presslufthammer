package de.tuberlin.dima.presslufthammer.data.dummy;

import java.util.List;

import com.google.common.collect.Lists;

import de.tuberlin.dima.presslufthammer.data.Field;
import de.tuberlin.dima.presslufthammer.data.FieldWriter;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class DummyFieldWriter extends FieldWriter {

    private List<Value> values;

    protected DummyFieldWriter(FieldWriter parent, SchemaNode schema) {
        super(parent, schema);
        values = Lists.newLinkedList();
    }

    @Override
    protected void writeFieldInternal(Field field, int repetitionLevel,
            int definitionLevel) {
        Value value = new Value(field, repetitionLevel, definitionLevel);
        values.add(value);

    }

    public void printToStdout() {
        System.out.println("COLUMN: " + getQualifiedName());
        for (Value value : values) {
            System.out.println("r: " + value.repetitionLevel + ", d: "
                    + value.definitionLevel + ", field: "
                    + value.field.toString());
        }
        for (FieldWriter childWriter : getChildren()) {
            DummyFieldWriter child = (DummyFieldWriter) childWriter;
            child.printToStdout();
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

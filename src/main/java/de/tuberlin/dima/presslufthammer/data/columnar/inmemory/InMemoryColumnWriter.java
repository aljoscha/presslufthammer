package de.tuberlin.dima.presslufthammer.data.columnar.inmemory;

import java.util.List;

import de.tuberlin.dima.presslufthammer.data.Field;
import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryTablet.ColumnEntry;

public class InMemoryColumnWriter implements ColumnWriter {
    private SchemaNode schema;
    private List<InMemoryTablet.ColumnEntry> column;

    protected InMemoryColumnWriter(SchemaNode schema,
            List<InMemoryTablet.ColumnEntry> column) {
        this.schema = schema;
        this.column = column;
    }

    public void writeField(Field field, int repetitionLevel, int definitionLevel) {
        field.writeToColumn(this, repetitionLevel, definitionLevel);
    }

    @Override
    public void writeInt32(int value, int repetitionLevel, int definitionLevel) {
        if (!schema.getPrimitiveType().equals(PrimitiveType.INT32)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        InMemoryTablet.ColumnEntry entry = new ColumnEntry(value,
                repetitionLevel, definitionLevel);
        column.add(entry);
    }

    @Override
    public void writeInt64(long value, int repetitionLevel, int definitionLevel) {
        if (!schema.getPrimitiveType().equals(PrimitiveType.INT64)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        InMemoryTablet.ColumnEntry entry = new ColumnEntry(value,
                repetitionLevel, definitionLevel);
        column.add(entry);
    }

    @Override
    public void writeBool(boolean value, int repetitionLevel,
            int definitionLevel) {
        if (!schema.getPrimitiveType().equals(PrimitiveType.BOOLEAN)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        InMemoryTablet.ColumnEntry entry = new ColumnEntry(value,
                repetitionLevel, definitionLevel);
        column.add(entry);
    }

    @Override
    public void writeFloat(float value, int repetitionLevel, int definitionLevel) {
        if (!schema.getPrimitiveType().equals(PrimitiveType.FLOAT)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        InMemoryTablet.ColumnEntry entry = new ColumnEntry(value,
                repetitionLevel, definitionLevel);
        column.add(entry);
    }

    @Override
    public void writeDouble(double value, int repetitionLevel,
            int definitionLevel) {
        if (!schema.getPrimitiveType().equals(PrimitiveType.DOUBLE)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        InMemoryTablet.ColumnEntry entry = new ColumnEntry(value,
                repetitionLevel, definitionLevel);
        column.add(entry);
    }

    @Override
    public void writeString(String value, int repetitionLevel,
            int definitionLevel) {
        if (!schema.getPrimitiveType().equals(PrimitiveType.STRING)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        InMemoryTablet.ColumnEntry entry = new ColumnEntry(value,
                repetitionLevel, definitionLevel);
        column.add(entry);
    }
    
    @Override
    public void writeNull(int repetitionLevel,
            int definitionLevel) {
        InMemoryTablet.ColumnEntry entry = new ColumnEntry(null,
                repetitionLevel, definitionLevel);
        column.add(entry);
    }
}

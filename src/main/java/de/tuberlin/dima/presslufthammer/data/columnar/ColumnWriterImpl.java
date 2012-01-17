package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.hierarchical.Field;

public class ColumnWriterImpl implements ColumnWriter {
    private SchemaNode schema;
    private DataOutputStream out;

    protected ColumnWriterImpl(SchemaNode schema, OutputStream outputStream)
            throws IOException {
        this.schema = schema;
        out = new DataOutputStream(new BufferedOutputStream(outputStream));
    }

    public void writeField(Field field, int repetitionLevel, int definitionLevel)
            throws IOException {
        field.writeToColumn(this, repetitionLevel, definitionLevel);
    }

    public void flush() throws IOException {
        out.flush();
    }
    
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void writeInt32(int value, int repetitionLevel, int definitionLevel)
            throws IOException {
        if (!schema.getPrimitiveType().equals(PrimitiveType.INT32)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        out.writeInt(repetitionLevel);
        out.writeInt(definitionLevel);
        out.writeInt(value);
    }

    @Override
    public void writeInt64(long value, int repetitionLevel, int definitionLevel)
            throws IOException {
        if (!schema.getPrimitiveType().equals(PrimitiveType.INT64)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        out.writeInt(repetitionLevel);
        out.writeInt(definitionLevel);
        out.writeLong(value);
    }

    @Override
    public void writeBool(boolean value, int repetitionLevel,
            int definitionLevel) throws IOException {
        if (!schema.getPrimitiveType().equals(PrimitiveType.INT64)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        out.writeInt(repetitionLevel);
        out.writeInt(definitionLevel);
        out.writeBoolean(value);
    }

    @Override
    public void writeFloat(float value, int repetitionLevel, int definitionLevel)
            throws IOException {
        if (!schema.getPrimitiveType().equals(PrimitiveType.INT64)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        out.writeInt(repetitionLevel);
        out.writeInt(definitionLevel);
        out.writeFloat(value);
    }

    @Override
    public void writeDouble(double value, int repetitionLevel,
            int definitionLevel) throws IOException {
        if (!schema.getPrimitiveType().equals(PrimitiveType.INT64)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        out.writeInt(repetitionLevel);
        out.writeInt(definitionLevel);
        out.writeDouble(value);
    }

    @Override
    public void writeString(String value, int repetitionLevel,
            int definitionLevel) throws IOException {
        if (!schema.getPrimitiveType().equals(PrimitiveType.STRING)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        out.writeInt(repetitionLevel);
        out.writeInt(definitionLevel);
        out.writeUTF(value);
    }

    @Override
    public void writeNull(int repetitionLevel, int definitionLevel)
            throws IOException {
        out.writeInt(repetitionLevel);
        out.writeInt(definitionLevel);
    }
}
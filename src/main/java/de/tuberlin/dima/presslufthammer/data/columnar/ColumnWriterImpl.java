package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.hierarchical.Field;

/**
 * Implementation of the {@link ColumnWriter} interface that writes column data
 * to an {@link OutputStream}. The data written by this class can be read again
 * by {@link ColumnReaderImpl}.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class ColumnWriterImpl implements ColumnWriter {
    private SchemaNode schema;
    private DataOutputStream out;

    /**
     * Constructs a column writer that reads from the given output stream.
     */
    public ColumnWriterImpl(SchemaNode schema, OutputStream outputStream)
            throws IOException {
        this.schema = schema;
        out = new DataOutputStream(new BufferedOutputStream(outputStream));
    }

    /**
     * Calls {@code flush} on the output stream, should be called before doing
     * further work on the written data.
     */
    public void flush() throws IOException {
        out.flush();
    }

    /**
     * Calls {@code close} on the output stream, should be called before doing
     * further work on the written data. Or when closing a {@link Tablet}
     * implementation.
     */
    public void close() throws IOException {
        out.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeField(Field field, int repetitionLevel, int definitionLevel)
            throws IOException {
        field.writeToColumn(this, repetitionLevel, definitionLevel);
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeNull(int repetitionLevel, int definitionLevel)
            throws IOException {
        out.writeInt(repetitionLevel);
        out.writeInt(definitionLevel);
    }
}

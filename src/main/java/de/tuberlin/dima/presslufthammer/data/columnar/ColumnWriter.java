package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

/**
 * A writer that is used to write data to a column of a {@link Tablet} . This is
 * returned by the method {@code getColumnWriter} of {@link Tablet}.
 * 
 * <p>
 * All the methods can possibly throw a {@link IOException} because writer might
 * try to write to a file.
 * 
 * <p>
 * For writer we do not have specific subclasses, as for ColumnReader, since the
 * writer is not very performance sensitive and it must not store the current
 * column value internally.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class ColumnWriter {
    private SchemaNode schema;
    private DataOutputStream out;

    /**
     * Constructs a column writer that reads from the given output stream.
     */
    public ColumnWriter(SchemaNode schema, OutputStream outputStream)
            throws IOException {
        this(schema, new DataOutputStream(
                new BufferedOutputStream(outputStream)));
    }

    /**
     * Constructs a column writer that reads from the given data output stream.
     */
    public ColumnWriter(SchemaNode schema, DataOutputStream outputStream)
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
     * Writes the given value to the column with the given repetition/definition
     * levels.
     */
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
     * Writes the given value to the column with the given repetition/definition
     * levels.
     */
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
     * Writes the given value to the column with the given repetition/definition
     * levels.
     */
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
     * Writes the given value to the column with the given repetition/definition
     * levels.
     */
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
     * Writes the given value to the column with the given repetition/definition
     * levels.
     */
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
     * Writes the given value to the column with the given repetition/definition
     * levels.
     */
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
     * Writes a null value to the column with the given repetition/definition
     * levels. Normally nothing is written for a null value because null values
     * can be determied based on the definition level and the definition level
     * of the associated schema.
     */
    public void writeNull(int repetitionLevel, int definitionLevel)
            throws IOException {
        out.writeInt(repetitionLevel);
        out.writeInt(definitionLevel);
    }
}

package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

/**
 * A reader for reading columnar data from the column of a {@link Tablet}. A
 * subclass of this is returned by the method {@code getColumnReader} of
 * {@link Tablet}.
 * 
 * <p>
 * All the methods can possibly throw a {@link IOException} because the column
 * data could be coming from a file.
 * 
 * <p>
 * Use advance() to advance the reader to the next value of the column, the
 * current value of the column and the repetition/definition levels can be
 * retrieved multiple times using the respective methods.
 * 
 * <p>
 * The data in that {@link InputStream} must of course be organized in such a
 * way that this class understands it, {@link ColumnWriter} writes column data
 * in such a way.
 * 
 * <p>
 * There is a specific implementation for every primitive data type.
 * 
 * @author Aljoscha Krettek
 * 
 */
public abstract class ColumnReader {
    protected SchemaNode schema;
    protected DataInputStream in;
    protected int nextRepetition = 0;
    protected int nextDefinition = 0;
    protected int currentRepetition = 0;
    protected int currentDefinition = 0;

    // for writing during query execution
    protected int currentWriteRepetition = 0;

    /**
     * Constructs a column reader from the given input stream.
     */
    public ColumnReader(SchemaNode schema, InputStream inputStream)
            throws IOException {
        this(schema, new DataInputStream(new BufferedInputStream(inputStream)));
    }

    /**
     * Constructs a column reader directly from the given data input stream.
     */
    public ColumnReader(SchemaNode schema, DataInputStream inputStream)
            throws IOException {
        this.schema = schema;
        this.in = inputStream;
        advanceLevels();
    }

    /**
     * Advances the current write repetition value, must be used if the value of
     * the column reader is used in a group field for example.
     */
    public void advanceWriteRepetition() {
        currentWriteRepetition = nextRepetition;
    }

    /**
     * Returns the current write repetition.
     */
    public int getWriteRepetition() {
        return currentWriteRepetition;
    }

    /**
     * Internal method that reads the next levels from the input stream and sets
     * both to -1 when EOF is reached.
     */
    protected void advanceLevels() throws IOException {
        currentRepetition = nextRepetition;
        currentDefinition = nextDefinition;
        try {
            nextRepetition = in.readInt();
            nextDefinition = in.readInt();
        } catch (EOFException e) {
            // end is reached, or something else went wrong
            nextRepetition = -1;
            nextDefinition = -1;
        }
        if (currentRepetition <= currentWriteRepetition) {
            currentWriteRepetition = currentRepetition;
        }
    }

    /**
     * Returns true when there is a next value available, the next value could
     * be {@code null} though.
     */
    public boolean hasNext() {
        return nextRepetition >= 0;
    }

    /**
     * Returns true when the current column value is NULL.
     */
    public boolean isNull() {
        return currentDefinition < schema.getMaxDefinition();
    }

    /**
     * Returns the current repetition level.
     */
    public int getCurrentRepetition() {
        return currentRepetition;
    }

    /**
     * Returns the current definition level.
     */
    public int getCurrentDefinition() {
        return currentDefinition;
    }

    /**
     * Returns the next repetition level without advancing the reader.
     */
    public int getNextRepetition() {
        if (hasNext()) {
            return nextRepetition;
        }
        return 0; // seems to work with the assembly fsm
    }

    /**
     * Returns the next definition level without advancing the reader.
     */
    public int getNextDefinition() {
        if (hasNext()) {
            return nextDefinition;
        }
        return 0; // seems to work with the assembly fsm
    }

    /**
     * Returns the current value of the reader.
     */
    public Object getValue() {
        if (isNull()) {
            return null;
        }

        if (schema.getPrimitiveType() == PrimitiveType.INT32) {
            return getInt32();
        } else if (schema.getPrimitiveType() == PrimitiveType.INT64) {
            return getInt64();
        } else if (schema.getPrimitiveType() == PrimitiveType.BOOLEAN) {
            return getBool();
        } else if (schema.getPrimitiveType() == PrimitiveType.FLOAT) {
            return getFloat();
        } else if (schema.getPrimitiveType() == PrimitiveType.DOUBLE) {
            return getDouble();
        } else if (schema.getPrimitiveType() == PrimitiveType.STRING) {
            return getString();
        }
        return null;
    }

    /**
     * Advances the reader to the next column value, if there is one.
     */
    public abstract void advance() throws IOException;

    /**
     * Returns the current value of the reader.
     */
    public int getInt32() {
        throw new RuntimeException("Column reader is no int32 reader.");
    }

    /**
     * Returns the current value of the reader.
     */
    public long getInt64() {
        throw new RuntimeException("Column reader is no int64 reader.");
    }

    /**
     * Returns the current value of the reader.
     */
    public boolean getBool() {
        throw new RuntimeException("Column reader is no bool reader.");
    }

    /**
     * Returns the current value of the reader.
     */
    public float getFloat() {
        throw new RuntimeException("Column reader is no float reader.");
    }

    /**
     * Returns the current value of the reader.
     */
    public double getDouble() {
        throw new RuntimeException("Column reader is no double reader.");
    }

    /**
     * Returns the current value of the reader.
     */
    public String getString() {
        throw new RuntimeException("Column reader is no String reader.");
    }

    /**
     * Writes the current value of the reader to the given column writer.
     */
    public abstract void writeToColumn(ColumnWriter writer) throws IOException;
}

package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.hierarchical.Field;

/**
 * This interface must be used when writing data to a column of a {@link Tablet}
 * . An object implementing this interface is returned by the method
 * {@code getColumnWriter} of {@link Tablet}.
 * 
 * <p>
 * All the methods can possibly throw a {@link IOException} because writer might
 * try to write to a file.
 * 
 * @author Aljoscha Krettek
 * 
 */
public interface ColumnWriter {
    /**
     * Invoked the {@code writeToColumn} method of the passed {@link Field} to
     * write that fields data to this column writer.
     */
    public void writeField(Field field, int repetitionLevel, int definitionLevel)
            throws IOException;

    /**
     * Writes the given value to the column with the given repetition/definition
     * levels.
     */
    public void writeInt32(int value, int repetitionLevel, int definitionLevel)
            throws IOException;

    /**
     * Writes the given value to the column with the given repetition/definition
     * levels.
     */
    public void writeInt64(long value, int repetitionLevel, int definitionLevel)
            throws IOException;

    /**
     * Writes the given value to the column with the given repetition/definition
     * levels.
     */
    public void writeBool(boolean value, int repetitionLevel,
            int definitionLevel) throws IOException;

    /**
     * Writes the given value to the column with the given repetition/definition
     * levels.
     */
    public void writeFloat(float value, int repetitionLevel, int definitionLevel)
            throws IOException;

    /**
     * Writes the given value to the column with the given repetition/definition
     * levels.
     */
    public void writeDouble(double value, int repetitionLevel,
            int definitionLevel) throws IOException;

    /**
     * Writes the given value to the column with the given repetition/definition
     * levels.
     */
    public void writeString(String value, int repetitionLevel,
            int definitionLevel) throws IOException;

    /**
     * Writes a null value to the column with the given repetition/definition
     * levels. Normally nothing is written for a null value because null values
     * can be determied based on the definition level and the definition level
     * of the associated schema.
     */
    public void writeNull(int repetitionLevel, int definitionLevel)
            throws IOException;
}

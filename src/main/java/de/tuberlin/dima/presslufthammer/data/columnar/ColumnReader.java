package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.IOException;

/**
 * This interface must be used when reading data form a column of a
 * {@link Tablet}. An object implementing this interface is returned by the
 * method {@code getColumnReader} of {@link Tablet}.
 * 
 * <p>
 * All the methods can possibly throw a {@link IOException} because the column
 * data could be coming from a file.
 * 
 * <p>
 * The next repetition/definition levels are stored internally and can be
 * retrieved multiple times with the respective methods without advancing the
 * reader. The {@code getNextValue} type methods advance the reader, however.
 * 
 * @author Aljoscha Krettek
 * 
 */
public interface ColumnReader {
    /**
     * Returns true when there is a next value available, the next value could
     * be {@code null} though.
     */
    public boolean hasNext() throws IOException;

    /**
     * Returns true when the next column value is {@code null}. This can be
     * determined from the next definition level and the definition level of the
     * {@link SchemaNode} of the column.
     */
    public boolean nextIsNull() throws IOException;

    /**
     * Returns the next value that can be read from the column and advances the
     * reader.
     */
    public Object getNextValue() throws IOException;

    /**
     * Returns the next repetition level without advancing the reader.
     */
    public int getNextRepetition() throws IOException;

    /**
     * Returns the next definition level without advancing the reader.
     */
    public int getNextDefinition() throws IOException;

    /**
     * Returns the next value of the reader as an {@code int}. Must only be
     * called on column readers of primitive type int32.
     */
    public int getNextInt32() throws IOException;

    /**
     * Returns the next value of the reader as an {@code long}. Must only be
     * called on column readers of primitive type int64.
     */
    public long getNextInt64() throws IOException;

    /**
     * Returns the next value of the reader as an {@code boolean}. Must only be
     * called on column readers of primitive type bool.
     */
    public boolean getNextBool() throws IOException;

    /**
     * Returns the next value of the reader as an {@code float}. Must only be
     * called on column readers of primitive type float.
     */
    public float getNextFloat() throws IOException;

    /**
     * Returns the next value of the reader as an {@code double}. Must only be
     * called on column readers of primitive type double.
     */
    public double getNextDouble() throws IOException;

    /**
     * Returns the next value of the reader as an {@code String}. Must only be
     * called on column readers of primitive type string.
     */
    public String getNextString() throws IOException;
}

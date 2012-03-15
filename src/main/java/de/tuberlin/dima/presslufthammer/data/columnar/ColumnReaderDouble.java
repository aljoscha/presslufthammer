package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

/**
 * Column reader implementation for double columns.
 * 
 * @author Aljoscha Krettek
 * 
 */
public final class ColumnReaderDouble extends ColumnReader {
    double currentValue = -1.0;

    /**
     * {@inheritDoc}
     */
    public ColumnReaderDouble(SchemaNode schema, DataInputStream inputStream)
            throws IOException {
        super(schema, inputStream);
    }

    /**
     * {@inheritDoc}
     */
    public ColumnReaderDouble(SchemaNode schema, InputStream inputStream)
            throws IOException {
        super(schema, inputStream);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void advance() throws IOException {
        if (hasNext()) {
            if (nextDefinition >= schema.getMaxDefinition()) {
                // not NULL
                currentValue = in.readDouble();
            }
            advanceLevels();
        } else {
            throw new RuntimeException(
                    "Has no next value, advance should not have been called.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble() {
        if (isNull()) {
            throw new RuntimeException(
                    "Current value is NULL, getDouble should not have been called.");
        }

        return currentValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeToColumn(ColumnWriter writer) throws IOException {
        if (isNull()) {
            writer.writeNull(currentWriteRepetition, currentDefinition);
        } else {
            writer.writeDouble(currentValue, currentWriteRepetition,
                    currentDefinition);
        }
        currentWriteRepetition = nextRepetition;
    }
}

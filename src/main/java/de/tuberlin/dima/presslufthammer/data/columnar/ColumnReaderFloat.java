package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

/**
 * Column reader implementation for float columns.
 * 
 * @author Aljoscha Krettek
 * 
 */
public final class ColumnReaderFloat extends ColumnReader {
    float currentValue = -1.0f;

    /**
     * {@inheritDoc}
     */
    public ColumnReaderFloat(SchemaNode schema, DataInputStream inputStream)
            throws IOException {
        super(schema, inputStream);
    }

    /**
     * {@inheritDoc}
     */
    public ColumnReaderFloat(SchemaNode schema, InputStream inputStream)
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
                currentValue = in.readFloat();
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
    public float getFloat() {
        if (isNull()) {
            throw new RuntimeException(
                    "Current value is NULL, getFloat should not have been called.");
        }

        return currentValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeToColumn(ColumnWriter writer) throws IOException {
        if (isNull()) {
            writer.writeNull(currentRepetition, currentDefinition);
        } else {
            writer.writeFloat(currentValue, currentRepetition,
                    currentDefinition);
        }
    }
}

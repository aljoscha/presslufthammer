package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

/**
 * Column reader implementation for int32 columns.
 * 
 * @author Aljoscha Krettek
 * 
 */
public final class ColumnReaderInt32 extends ColumnReader {
    int currentValue = -1;

    /**
     * {@inheritDoc}
     */
    public ColumnReaderInt32(SchemaNode schema, DataInputStream inputStream)
            throws IOException {
        super(schema, inputStream);
    }

    /**
     * {@inheritDoc}
     */
    public ColumnReaderInt32(SchemaNode schema, InputStream inputStream)
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
                currentValue = in.readInt();
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
    public int getInt32() {
        if (isNull()) {
            throw new RuntimeException(
                    "Current value is NULL, getInt32 should not have been called.");
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
            writer.writeInt32(currentValue, currentRepetition,
                    currentDefinition);
        }
    }
}

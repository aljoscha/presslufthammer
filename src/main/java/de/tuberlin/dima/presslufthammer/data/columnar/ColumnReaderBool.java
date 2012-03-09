package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;

/**
 * Column reader implementation for bool columns.
 * 
 * @author Aljoscha Krettek
 * 
 */
public final class ColumnReaderBool extends ColumnReader {
    boolean currentValue = false;

    /**
     * {@inheritDoc}
     */
    public ColumnReaderBool(SchemaNode schema, DataInputStream inputStream)
            throws IOException {
        super(schema, inputStream);
    }

    /**
     * {@inheritDoc}
     */
    public ColumnReaderBool(SchemaNode schema, InputStream inputStream)
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
                currentValue = in.readBoolean();
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
    public boolean getBool() {
        if (isNull()) {
            throw new RuntimeException(
                    "Current value is NULL, getBool should not have been called.");
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
            writer.writeBool(currentValue, currentRepetition, currentDefinition);
        }
    }
}

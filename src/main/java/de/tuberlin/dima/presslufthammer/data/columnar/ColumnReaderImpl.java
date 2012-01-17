package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class ColumnReaderImpl implements ColumnReader {
    private SchemaNode schema;
    private DataInputStream in;
    private int nextRepetition;
    private int nextDefinition;

    protected ColumnReaderImpl(SchemaNode schema, InputStream inputStream)
            throws IOException {
        this.schema = schema;
        in = new DataInputStream(new BufferedInputStream(inputStream));
        readNextLevels();
    }

    public SchemaNode getSchema() {
        return schema;
    }

    private void readNextLevels() throws IOException {
        try {
            nextRepetition = in.readInt();
            nextDefinition = in.readInt();
        } catch (EOFException e) {
            // end is reached, or something else went wrong
            nextRepetition = -1;
            nextDefinition = -1;
        }
    }

    @Override
    public boolean hasNext() {
        return nextRepetition >= 0;
    }

    @Override
    public boolean nextIsNull() {
        return nextDefinition < schema.getMaxDefinition();
    }

    @Override
    public int getNextRepetition() {
        if (hasNext()) {
            return nextRepetition;
        }
        return 0; // seems to work with the assembly fsm
    }

    @Override
    public int getNextDefinition() {
        if (hasNext()) {
            return nextDefinition;
        }
        return 0; // seems to work with the assembly fsm
    }

    @Override
    public Object getNextValue() throws IOException {
        if (hasNext()) {
            if (nextIsNull()) {
                readNextLevels();
                return null;
            }
            if (schema.getPrimitiveType() == PrimitiveType.INT32) {
                return getNextInt32();
            } else if (schema.getPrimitiveType() == PrimitiveType.INT64) {
                return getNextInt64();
            } else if (schema.getPrimitiveType() == PrimitiveType.BOOLEAN) {
                return getNextBool();
            } else if (schema.getPrimitiveType() == PrimitiveType.FLOAT) {
                return getNextFloat();
            } else if (schema.getPrimitiveType() == PrimitiveType.DOUBLE) {
                return getNextDouble();
            } else if (schema.getPrimitiveType() == PrimitiveType.STRING) {
                return getNextString();
            }
        }
        return null;
    }

    @Override
    public int getNextInt32() throws IOException {
        if (hasNext()) {
            if (nextIsNull()) {
                throw new RuntimeException("Next value of reader "
                        + schema.getQualifiedName()
                        + " is NULL, get* should not have been called.");
            }
            int result = 0;
            result = in.readInt();
            readNextLevels();
            return result;
        }
        throw new RuntimeException(
                "Has no next value, getNext* should not have been called.");
    }

    @Override
    public long getNextInt64() throws IOException {
        if (hasNext()) {
            if (nextIsNull()) {
                throw new RuntimeException("Next value of reader "
                        + schema.getQualifiedName()
                        + " is NULL, get* should not have been called.");
            }
            long result = 0;
            result = in.readLong();
            readNextLevels();
            return result;
        }
        throw new RuntimeException(
                "Has no next value, getNext* should not have been called.");
    }

    @Override
    public boolean getNextBool() throws IOException {
        if (hasNext()) {
            if (nextIsNull()) {
                throw new RuntimeException("Next value of reader "
                        + schema.getQualifiedName()
                        + " is NULL, get* should not have been called.");
            }
            boolean result = false;
            result = in.readBoolean();
            readNextLevels();
            return result;
        }
        throw new RuntimeException(
                "Has no next value, getNext* should not have been called.");
    }

    @Override
    public float getNextFloat() throws IOException {
        if (hasNext()) {
            if (nextIsNull()) {
                throw new RuntimeException("Next value of reader "
                        + schema.getQualifiedName()
                        + " is NULL, get* should not have been called.");
            }
            float result = 0.0f;
            result = in.readFloat();
            readNextLevels();
            return result;
        }
        throw new RuntimeException(
                "Has no next value, getNext* should not have been called.");
    }

    @Override
    public double getNextDouble() throws IOException {
        if (hasNext()) {
            if (nextIsNull()) {
                throw new RuntimeException("Next value of reader "
                        + schema.getQualifiedName()
                        + " is NULL, get* should not have been called.");
            }
            double result = 0.0;
            result = in.readDouble();
            readNextLevels();
            return result;
        }
        throw new RuntimeException(
                "Has no next value, getNext* should not have been called.");
    }

    @Override
    public String getNextString() throws IOException {
        if (hasNext()) {
            if (nextIsNull()) {
                throw new RuntimeException("Next value of reader "
                        + schema.getQualifiedName()
                        + " is NULL, get* should not have been called.");
            }
            String result = null;
            result = in.readUTF();
            readNextLevels();
            return result;
        }
        throw new RuntimeException(
                "Has no next value, getNext* should not have been called.");
    }
}

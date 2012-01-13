package de.tuberlin.dima.presslufthammer.data.columnar.ondisk;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;

public class OnDiskColumnReader implements ColumnReader {
    private SchemaNode schema;
    private BufferedReader reader;
    private int nextRepetition;
    private int nextDefinition;

    protected OnDiskColumnReader(SchemaNode schema, File file)
            throws IOException {
        this.schema = schema;
        FileInputStream fStream = new FileInputStream(file);
        reader = new BufferedReader(new InputStreamReader(fStream));
        readNextLevels();
    }

    public SchemaNode getSchema() {
        return schema;
    }

    private void readNextLevels() {
        String repetitionLine = null;
        String definitionLine = null;
        try {
            repetitionLine = reader.readLine();
            definitionLine = reader.readLine();
        } catch (IOException e) {
        }

        if (repetitionLine == null || definitionLine == null) {
            nextRepetition = -1;
            nextDefinition = -1;
            return;
        }

        nextRepetition = Integer.parseInt(repetitionLine);
        nextDefinition = Integer.parseInt(definitionLine);
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
    public Object getNextValue() {
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
    public int getNextInt32() {
        if (hasNext()) {
            if (nextIsNull()) {
                throw new RuntimeException("Next value of reader " + schema.getQualifiedName() + " is NULL, get* should not have been called.");
            }
            try {
                String line = reader.readLine();
                readNextLevels();
                return Integer.parseInt(line);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new RuntimeException("Has no next value, getNext* should not have been called.");
    }

    @Override
    public long getNextInt64() {
        if (hasNext()) {
            if (nextIsNull()) {
                throw new RuntimeException("Next value of reader " + schema.getQualifiedName() + " is NULL, get* should not have been called.");
            }
            try {
                String line = reader.readLine();
                readNextLevels();
                return Long.parseLong(line);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new RuntimeException("Has no next value, getNext* should not have been called.");
    }

    @Override
    public boolean getNextBool() {
        if (hasNext()) {
            if (nextIsNull()) {
                throw new RuntimeException("Next value of reader " + schema.getQualifiedName() + " is NULL, get* should not have been called.");
            }
            try {
                String line = reader.readLine();
                readNextLevels();
                return Boolean.parseBoolean(line);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new RuntimeException("Has no next value, getNext* should not have been called.");
    }

    @Override
    public float getNextFloat() {
        if (hasNext()) {
            if (nextIsNull()) {
                throw new RuntimeException("Next value of reader " + schema.getQualifiedName() + " is NULL, get* should not have been called.");
            }
            try {
                String line = reader.readLine();
                readNextLevels();
                return Float.parseFloat(line);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new RuntimeException("Has no next value, getNext* should not have been called.");
    }

    @Override
    public double getNextDouble() {
        if (hasNext()) {
            if (nextIsNull()) {
                throw new RuntimeException("Next value of reader " + schema.getQualifiedName() + " is NULL, get* should not have been called.");
            }
            try {
                String line = reader.readLine();
                readNextLevels();
                return Double.parseDouble(line);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new RuntimeException("Has no next value, getNext* should not have been called.");
    }

    @Override
    public String getNextString() {
        if (hasNext()) {
            if (nextIsNull()) {
                throw new RuntimeException("Next value of reader " + schema.getQualifiedName() + " is NULL, get* should not have been called.");
            }
            try {
                String line = reader.readLine();
                readNextLevels();
                return line;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new RuntimeException("Has no next value, getNext* should not have been called.");
    }
}

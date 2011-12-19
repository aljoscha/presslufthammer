package de.tuberlin.dima.presslufthammer.data.columnar.inmemory;

import java.util.Iterator;
import java.util.List;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;

public class InMemoryColumnReader implements ColumnReader {
    private SchemaNode schema;
    private Iterator<InMemoryTablet.ColumnEntry> iterator;
    private InMemoryTablet.ColumnEntry currentEntry = null;

    protected InMemoryColumnReader(SchemaNode schema,
            List<InMemoryTablet.ColumnEntry> column) {
        this.schema = schema;
        iterator = column.iterator();
        if (iterator.hasNext()) {
            currentEntry = iterator.next();
        }
    }

    public SchemaNode getSchema() {
        return schema;
    }

    @Override
    public boolean hasNext() {
        return currentEntry != null;
    }

    @Override
    public int getNextRepetition() {
        if (currentEntry != null) {
            return currentEntry.repetitionLevel;
        } else {
            return 0;
        }
    }

    @Override
    public int getNextDefinition() {
        if (currentEntry != null) {
            return currentEntry.definitionLevel;
        } else {
            return 0;
        }
    }

    @Override
    public Object getNextValue() {
        Object result = currentEntry.value;
        if (iterator.hasNext()) {
            currentEntry = iterator.next();
        } else {
            currentEntry = null;
        }
        return result;
    }

    @Override
    public int getNextInt32() {
        int result = (Integer) currentEntry.value;
        if (iterator.hasNext()) {
            currentEntry = iterator.next();
        } else {
            currentEntry = null;
        }
        return result;
    }

    @Override
    public long getNextInt64() {
        long result = (Long) currentEntry.value;
        if (iterator.hasNext()) {
            currentEntry = iterator.next();
        } else {
            currentEntry = null;
        }
        return result;
    }

    @Override
    public boolean getNextBool() {
        boolean result = (Boolean) currentEntry.value;
        if (iterator.hasNext()) {
            currentEntry = iterator.next();
        } else {
            currentEntry = null;
        }
        return result;
    }

    @Override
    public float getNextFloat() {
        float result = (Float) currentEntry.value;
        if (iterator.hasNext()) {
            currentEntry = iterator.next();
        } else {
            currentEntry = null;
        }
        return result;
    }

    @Override
    public double getNextDouble() {
        double result = (Double) currentEntry.value;
        if (iterator.hasNext()) {
            currentEntry = iterator.next();
        } else {
            currentEntry = null;
        }
        return result;
    }

    @Override
    public String getNextString() {
        String result = (String) currentEntry.value;
        if (iterator.hasNext()) {
            currentEntry = iterator.next();
        } else {
            currentEntry = null;
        }
        return result;
    }
}

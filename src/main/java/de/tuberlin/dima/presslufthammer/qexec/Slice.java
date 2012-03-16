package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;

public class Slice {
    private Map<SchemaNode, ColumnReader> columns;

    private int fetchLevel = 0;

    public Slice(Tablet sourceTablet, List<SchemaNode> selectedFields) {
        columns = Maps.newHashMap();
        for (SchemaNode field : selectedFields) {
            if (field.isPrimitive()) {
                columns.put(field, sourceTablet.getColumnReader(field));
            }
        }
    }

    public int getFetchLevel() {
        return fetchLevel;
    }

    public boolean hasNext() throws IOException {
        for (ColumnReader column : columns.values()) {
            if (column.hasNext()) {
                return true;
            }
        }
        return false;
    }

    public void fetch() throws IOException {
        int nextLevel = 0;
        for (ColumnReader column : columns.values()) {
            if (column.hasNext() && column.getNextRepetition() >= fetchLevel) {
                column.advance();
            }
            nextLevel = Math.max(column.getNextRepetition(), nextLevel);
        }
        fetchLevel = nextLevel;
    }

    public ColumnReader getColumn(SchemaNode schema) {
        return columns.get(schema);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("SLICE: --------------------------------------\n");
        result.append("FETCH-LEVEL: " + fetchLevel + "\n");
        for (ColumnReader column : columns.values()) {
            result.append(column + "\n");
            if (column.isNull()) {

                return "rep: " + column.getCurrentRepetition() + " def: "
                        + column.getCurrentDefinition() + " val: NULL";
            }
            return "rep: " + column.getCurrentRepetition() + " def: "
                    + column.getCurrentDefinition() + " val: "
                    + column.getValue();
        }
        return result.toString();
    }
}

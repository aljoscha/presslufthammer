package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;

public class Slice {
    private Map<SchemaNode, SliceColumn> columns;

    private int fetchLevel = 0;

    public Slice(Tablet sourceTablet, List<SchemaNode> selectedFields) {
        columns = Maps.newHashMap();
        for (SchemaNode field : selectedFields) {
            if (field.isPrimitive()) {
                switch (field.getPrimitiveType()) {
                case INT32:
                    columns.put(
                            field,
                            new Int32SliceColumn(sourceTablet
                                    .getColumnReader(field)));
                    break;
                case INT64:
                    columns.put(
                            field,
                            new Int64SliceColumn(sourceTablet
                                    .getColumnReader(field)));
                    break;
                case FLOAT:
                    columns.put(
                            field,
                            new FloatSliceColumn(sourceTablet
                                    .getColumnReader(field)));
                    break;
                case DOUBLE:
                    columns.put(
                            field,
                            new DoubleSliceColumn(sourceTablet
                                    .getColumnReader(field)));
                    break;
                case BOOLEAN:
                    columns.put(
                            field,
                            new BoolSliceColumn(sourceTablet
                                    .getColumnReader(field)));
                    break;
                case STRING:
                    columns.put(
                            field,
                            new StringSliceColumn(sourceTablet
                                    .getColumnReader(field)));
                    break;
                }
            }
        }
    }

    public int getFetchLevel() {
        return fetchLevel;
    }

    public boolean hasNext() throws IOException {
        for (SliceColumn column : columns.values()) {
            if (column.hasNext()) {
                return true;
            }
        }
        return false;
    }

    public void fetch() throws IOException {
        int nextLevel = 0;
        for (SliceColumn column : columns.values()) {
            if (column.getNextRepetition() >= fetchLevel) {
                column.advance();
            }
            nextLevel = Math.max(column.getNextRepetition(), nextLevel);
        }
        fetchLevel = nextLevel;
    }

    public SliceColumn getColumn(SchemaNode schema) {
        return columns.get(schema);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("SLICE: --------------------------------------\n");
        result.append("FETCH-LEVEL: " + fetchLevel + "\n");
        for (SliceColumn column : columns.values()) {
            result.append(column + "\n");
        }
        return result.toString();
    }
}

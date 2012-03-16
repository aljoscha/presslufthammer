package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;

/**
 * A slice is used to advance column readers in lockstep according to a fetch
 * level. (See dremel paper for (some superficial) details on this process)
 * 
 * <p>
 * The advance method advances the column readers, getColumn can be used to
 * retrieve a specific column reader.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class Slice {
    private Map<SchemaNode, ColumnReader> columns;

    private int fetchLevel = 0;

    /**
     * Initializes the column readers from the given tablet with only readers
     * for the fields in selectedFields.
     */
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

    /**
     * Returns true if any of the column readers can be advanced.
     */
    public boolean hasNext() throws IOException {
        for (ColumnReader column : columns.values()) {
            if (column.hasNext()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Advances the readers and adjusts the fetch level.
     */
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

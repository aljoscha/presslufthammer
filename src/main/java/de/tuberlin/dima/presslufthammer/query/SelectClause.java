package de.tuberlin.dima.presslufthammer.query;

/**
 * Representation of one where clause of a query.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class SelectClause {
    private final boolean isAll;
    private final String column;
    private final String renamedColumn;
    private final Aggregation aggregation;
    private final String renameAs;

    public SelectClause(String column, Aggregation aggregation, String renameAs) {
        this.column = column;
        this.aggregation = aggregation;
        this.renameAs = renameAs;
        if (column.equals("*")) {
            isAll = true;
        } else {
            isAll = false;
        }
        if (renameAs != null) {
            int lastDot = column.lastIndexOf('.');
            renamedColumn = column.substring(0, lastDot + 1) + renameAs;
        } else {
            renamedColumn = column;
        }
    }

    public boolean isAll() {
        return isAll;
    }

    public String getColumn() {
        return column;
    }

    public String getRenamedColumn() {
        return renamedColumn;
    }

    public Aggregation getAggregation() {
        return aggregation;
    }

    public String getRenameAs() {
        return renameAs;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        if (aggregation != Aggregation.NONE) {
            result.append(aggregation + "(");
        }
        result.append(column);
        if (aggregation != Aggregation.NONE) {
            result.append(")");
        }
        if (renameAs != null) {
            result.append(" AS " + renameAs);
        }
        return result.toString();
    }

    public static enum Aggregation {
        NONE, COUNT, SUM
    }
}

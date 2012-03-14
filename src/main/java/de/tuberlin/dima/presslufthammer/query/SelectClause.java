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
    }

    public boolean isAll() {
        return isAll;
    }

    public String getColumn() {
        return column;
    }

    public Aggregation getAggregation() {
        return aggregation;
    }

    public String getRenameAs() {
        return renameAs;
    }

    @Override
    public String toString() {
        if (renameAs != null) {
            return column + " AS " + renameAs;
        }
        return column;
    }

    public static enum Aggregation {
        NONE, COUNT, SUM
    }
}

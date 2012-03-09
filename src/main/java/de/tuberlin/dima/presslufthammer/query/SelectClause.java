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
    private final String renameAs;

    public SelectClause(String column, String renameAs) {
        this.column = column;
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
}

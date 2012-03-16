package de.tuberlin.dima.presslufthammer.qexec.grouping;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

/**
 * Subclasses of this class are used to store the values of various fields in
 * case of grouping/aggregation. There must be one subclass per data-type and
 * aggregation function. (A bit unfortunate but this "hack" works for now.)
 * 
 * <p>
 * The "plain" fields are for grouping fields, the "sum" and "count" fields are
 * for the respective aggregation functions.
 * 
 * @author Aljoscha Krettek
 * 
 */
public abstract class GroupField {
    protected ColumnWriter writer;

    /**
     * Instantiates a new GroupField that writes to the given writer when the
     * emit method is called.
     */
    public GroupField(ColumnWriter writer) {
        this.writer = writer;
    }

    /**
     * Adds a new value to this group vield, the behaviour depends on the
     * subclass, plain fields do only ever add one value, sum adds up values,
     * count just counts.
     */
    public abstract void addValue(ColumnReader reader);

    /**
     * Emits the current value to the column writer.
     */
    public abstract void emit() throws IOException;
}

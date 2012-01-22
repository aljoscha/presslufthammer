package de.tuberlin.dima.presslufthammer.data.hierarchical;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

/**
 * Class representing one field in a hierarchical record. Objects implementing
 * this interface are returned from {@link FieldIterator}. The field stores the
 * corresponding {@link SchemaNode}. This is mainly used by the
 * {@link ColumnStriper} while striping hierarchical records to {@link Tablet}S.
 * 
 * <p>
 * There are subclasses for all primitive types and record fields.
 * 
 * @author Aljoscha Krettek
 * 
 */
public abstract class Field {
    protected final SchemaNode schema;

    public Field(SchemaNode schema) {
        this.schema = schema;
    }

    public SchemaNode getSchema() {
        return schema;
    }

    public abstract boolean isPrimitive();

    /**
     * Write the value of this field to a given {@link ColumnWriter}
     */
    public abstract void writeToColumn(ColumnWriter writer,
            int repetitionLevel, int definitionLevel) throws IOException;
}

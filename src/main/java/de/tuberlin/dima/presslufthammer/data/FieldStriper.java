package de.tuberlin.dima.presslufthammer.data;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.hierarchical.Field;
import de.tuberlin.dima.presslufthammer.data.hierarchical.FieldIterator;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordDecoder;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordIterator;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordStore;
import de.tuberlin.dima.presslufthammer.data.hierarchical.fields.RecordField;

/**
 * This class implements the algorithm given in the Dremel paper for dissecting
 * records to columnar data. Records are read from a given {@link RecordStore}
 * and the columnar data is written to the given {@link Tablet}.
 * 
 * @author Aljoscha Krettek
 * 
 */
public final class FieldStriper {
    private SchemaNode schema;
    private FieldWriter rootWriter;

    /**
     */
    public FieldStriper(SchemaNode schema) {
        this.schema = schema;
    }

    /**
     * Dissects records from the given record store to the target tablet. Until
     * either the record iterator is depleted or the given number of records has
     * been dissected.
     * 
     * Returns true when the record iterator was not depleted by this run of
     * dissecting.
     */
    public boolean dissectRecords(RecordIterator records, Tablet targetTablet,
            int numRecords) throws IOException {
        rootWriter = createWriterTree(null, this.schema, targetTablet);
        RecordIterator iterator = records;
        RecordDecoder decoder = iterator.next();
        int count = 0;
        while (decoder != null) {
            dissectRecord(decoder, rootWriter, 0);
            count++;
            if (count >= numRecords && numRecords > 0) {
                break;
            }
            decoder = iterator.next();
        }
        rootWriter.finalizeLevels();
        return decoder != null;
    }

    /**
     * Dissects one single record.
     */
    private void dissectRecord(RecordDecoder decoder, FieldWriter writer,
            int repetitionLevel) throws IOException {
        Set<SchemaNode> seenFields = Sets.newHashSet();
        FieldIterator fieldIterator = decoder.fieldIterator();
        Field field = fieldIterator.next();
        while (field != null) {
            FieldWriter childWriter = writer.getChild(field.getSchema());
            int childRepetitionLevel = repetitionLevel;
            if (seenFields.contains(field.getSchema())) {
                childRepetitionLevel = childWriter.getRepetition();
            } else {
                seenFields.add(field.getSchema());
            }
            if (field.getSchema().isPrimitive()) {
                childWriter.writeField(field, childRepetitionLevel);
            } else {
                RecordField record = (RecordField) field;
                childWriter.writeField(null, childRepetitionLevel);
                dissectRecord(
                        decoder.newDecoder(field.getSchema(), record.getData()),
                        childWriter, childRepetitionLevel);
            }
            field = fieldIterator.next();
        }
    }

    /**
     * Constructs the tree of field writers required by the algorithm.
     */
    private FieldWriter createWriterTree(FieldWriter parent, SchemaNode schema,
            Tablet targetTablet) {
        ColumnWriter columnWriter = null;
        if (schema.isPrimitive()) {
            columnWriter = targetTablet.getColumnWriter(schema);
        }
        FieldWriter fieldWriter = new FieldWriter(parent, schema, columnWriter);
        if (schema.isRecord()) {
            for (SchemaNode childSchema : schema.getFieldList()) {
                FieldWriter childWriter = createWriterTree(fieldWriter,
                        childSchema, targetTablet);
                fieldWriter.addChild(childSchema, childWriter);
            }
        }
        return fieldWriter;
    }
}

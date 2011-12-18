package de.tuberlin.dima.presslufthammer.data;

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

public final class FieldStriper {
    private SchemaNode schema;
    private Tablet tablet;
    private FieldWriter rootWriter;

    public FieldStriper(SchemaNode schema, Tablet targetTablet) {
        this.schema = schema;
        this.tablet = targetTablet;

        rootWriter = createWriterTree(null, this.schema);
    }

    public void dissectRecords(RecordStore records) {
        RecordIterator iterator = records.recordIterator();
        RecordDecoder decoder = iterator.next();
        while (decoder != null) {
            dissectRecord(decoder, rootWriter, 0);
            decoder = iterator.next();
        }
        rootWriter.finalizeLevels();
    }

    private void dissectRecord(RecordDecoder decoder, FieldWriter writer,
            int repetitionLevel) {
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

    private FieldWriter createWriterTree(FieldWriter parent, SchemaNode schema) {
        ColumnWriter columnWriter = tablet.getColumnWriter(schema);
        FieldWriter fieldWriter = new FieldWriter(parent, schema, columnWriter);
        if (schema.isRecord()) {
            for (SchemaNode childSchema : schema.getFieldList()) {
                FieldWriter childWriter = createWriterTree(fieldWriter,
                        childSchema);
                fieldWriter.addChild(childSchema, childWriter);
            }
        }
        return fieldWriter;
    }
}

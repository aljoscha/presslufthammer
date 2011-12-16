package de.tuberlin.dima.presslufthammer.data;

import java.util.Set;

import com.google.common.collect.Sets;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.fields.RecordField;

public final class FieldStriper {
    private SchemaNode schema;
    private Tablet tablet;
    private FieldWriter rootWriter;

    public FieldStriper(SchemaNode schema, Tablet targetTablet) {
        this.schema = schema;
        this.tablet = targetTablet;

        rootWriter = createWriterTree(null, this.schema);
    }

    public void dissectRecords(RecordProvider recordProvider) {
        RecordDecoder decoder = recordProvider.next();
        while (decoder != null) {
            dissectRecord(decoder, rootWriter, 0);
            decoder = recordProvider.next();
        }
        rootWriter.finalizeLevels();
    }

    private void dissectRecord(RecordDecoder decoder, FieldWriter writer,
            int repetitionLevel) {
        Set<SchemaNode> seenFields = Sets.newHashSet();
        Field field = decoder.next();
        while (field != null) {
            FieldWriter childWriter = writer.getChild(field.schema);
            int childRepetitionLevel = repetitionLevel;
            if (seenFields.contains(field.schema)) {
                childRepetitionLevel = childWriter.getRepetition();
            } else {
                seenFields.add(field.schema);
            }
            if (field.schema.isPrimitive()) {
                childWriter.writeField(field, childRepetitionLevel);
            } else {
                RecordField record = (RecordField) field;
                childWriter.writeField(null, childRepetitionLevel);
                dissectRecord(
                        decoder.newDecoder(field.schema, record.getData()),
                        childWriter, childRepetitionLevel);
            }
            field = decoder.next();
        }
    }

    private FieldWriter createWriterTree(FieldWriter parent, SchemaNode schema) {
        ColumnWriter columnWriter = tablet.getColumnWriter(schema);
        FieldWriter fieldWriter = new FieldWriter(parent, schema, columnWriter);
        if (schema.isRecord()) {
            for (SchemaNode childSchema : schema.getFieldList()) {
                FieldWriter childWriter = createWriterTree(fieldWriter, childSchema);
                fieldWriter.addChild(childSchema, childWriter);
            }
        }
        return fieldWriter;
    }
}

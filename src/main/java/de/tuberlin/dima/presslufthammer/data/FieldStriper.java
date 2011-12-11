package de.tuberlin.dima.presslufthammer.data;

import java.util.Set;

import com.google.common.collect.Sets;

import de.tuberlin.dima.presslufthammer.data.dummy.DummyFieldWriter;
import de.tuberlin.dima.presslufthammer.data.fields.RecordField;

public class FieldStriper {
    private SchemaNode schema;
    private FieldWriterFactory fieldWriterFactory;
    private FieldWriter rootWriter;

    public FieldStriper(SchemaNode schema,
            FieldWriterFactory fieldWriterFactory) {
        this.schema = schema;
        this.fieldWriterFactory = fieldWriterFactory;

        rootWriter = createWriterTree(null, this.schema,
                this.fieldWriterFactory);
    }

    public void dissectRecords(RecordProvider recordProvider) {
        RecordDecoder decoder = recordProvider.next();
        while (decoder != null) {
            dissectRecord(decoder, rootWriter, 0);
            decoder = recordProvider.next();
        }
        rootWriter.finalizeLevels();
        
        DummyFieldWriter dummy = (DummyFieldWriter) rootWriter;
        dummy.printToStdout();
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
                dissectRecord(decoder.newDecoder(field.schema,
                        record.getData()), childWriter,
                        childRepetitionLevel);
            }
            field = decoder.next();
        }
    }

    private FieldWriter createWriterTree(FieldWriter parent, SchemaNode schema,
            FieldWriterFactory fieldWriterFactory) {
        FieldWriter writer = fieldWriterFactory.createFieldWriter(parent,
                schema);
        if (schema.isRecord()) {
            for (SchemaNode childSchema : schema.getFieldList()) {
                FieldWriter childWriter = createWriterTree(writer, childSchema,
                        fieldWriterFactory);
                writer.addChild(childSchema, childWriter);
            }
        }
        return writer;
    }
}

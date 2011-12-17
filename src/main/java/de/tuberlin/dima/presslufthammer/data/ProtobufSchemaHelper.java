package de.tuberlin.dima.presslufthammer.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.dyuproject.protostuff.parser.Field;
import com.dyuproject.protostuff.parser.Message;
import com.dyuproject.protostuff.parser.MessageField;
import com.dyuproject.protostuff.parser.Proto;
import com.dyuproject.protostuff.parser.ProtoUtil;

public class ProtobufSchemaHelper {
    public static SchemaNode readSchema(String filename, String messageName) {
        Proto proto = ProtoUtil.parseProto(new File(filename));
        Message message = proto.getMessage(messageName);
        return createRecordSchema(message);
    }

    private static SchemaNode createRecordSchema(Message message) {
        SchemaNode schema = SchemaNode.createRecord(message.getName());

        for (Field<?> field : message.getFields()) {
            SchemaNode fieldSchema = createFieldSchema(field);
            schema.addField(fieldSchema);
        }

        return schema;
    }

    private static SchemaNode createFieldSchema(Field<?> field) {
        SchemaNode schema = null;

        if (field.isMessageField()) {
            MessageField messageField = (MessageField) field;
            Message message = messageField.getMessage();
            schema = createRecordSchema(message);
        } else {
            PrimitiveType type = null;
            if (field instanceof Field.Int32) {
                type = PrimitiveType.INT32;
            } else if (field instanceof Field.Int64) {
                type = PrimitiveType.INT64;
            } else if (field instanceof Field.Bool) {
                type = PrimitiveType.BOOLEAN;
            } else if (field instanceof Field.Float) {
                type = PrimitiveType.FLOAT;
            } else if (field instanceof Field.Double) {
                type = PrimitiveType.DOUBLE;
            } else if (field instanceof Field.String) {
                type = PrimitiveType.STRING;
            }
            schema = SchemaNode.createPrimitive(field.getName(), type);
        }

        if (field.isOptional()) {
            schema.setOptional();
        } else if (field.isRepeated()) {
            schema.setRepeated();
        }

        return schema;
    }

    public static void writeSchema(String filename, SchemaNode schema)
            throws IOException {
        String schemaString = schema.toString();

        BufferedWriter out = new BufferedWriter(new FileWriter(filename));
        out.write(schemaString);
        out.close();
    }
}
package de.tuberlin.dima.presslufthammer.data.json;

import org.json.simple.JSONObject;

import de.tuberlin.dima.presslufthammer.data.Field;
import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.fields.BooleanField;
import de.tuberlin.dima.presslufthammer.data.fields.DoubleField;
import de.tuberlin.dima.presslufthammer.data.fields.FloatField;
import de.tuberlin.dima.presslufthammer.data.fields.IntField;
import de.tuberlin.dima.presslufthammer.data.fields.LongField;
import de.tuberlin.dima.presslufthammer.data.fields.RecordField;
import de.tuberlin.dima.presslufthammer.data.fields.StringField;

public class JSONHelper {

    public static Field createField(Object value, SchemaNode childSchema) {
        if (childSchema.isPrimitive()) {
            return createPrimitiveField(value, childSchema);
        } else {
            if (value instanceof JSONObject) {
                JSONObject jsonValue = (JSONObject) value;
                RecordField field = new RecordField(childSchema, jsonValue);
                return field;
            } else {
                return null;
            }
        }
    }

    public static Field createPrimitiveField(Object value, SchemaNode childSchema) {
        if (childSchema.getPrimitiveType() == PrimitiveType.INT32
                && value instanceof Integer) {
            return new IntField(childSchema, (Integer) value);
        } else if (childSchema.getPrimitiveType() == PrimitiveType.BOOLEAN
                && value instanceof Boolean) {
            return new BooleanField(childSchema, (Boolean) value);
        } else if (childSchema.getPrimitiveType() == PrimitiveType.INT64
                && value instanceof Long) {
            return new LongField(childSchema, (Long) value);
        } else if (childSchema.getPrimitiveType() == PrimitiveType.FLOAT
                && value instanceof Float) {
            return new FloatField(childSchema, (Float) value);
        } else if (childSchema.getPrimitiveType() == PrimitiveType.DOUBLE
                && value instanceof Double) {
            return new DoubleField(childSchema, (Double) value);
        } else if (childSchema.getPrimitiveType() == PrimitiveType.STRING
                && value instanceof String) {
            return new StringField(childSchema, (String) value);
        }
        return null;
    }
}

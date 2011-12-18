package de.tuberlin.dima.presslufthammer.data.hierarchical.json;

import org.json.simple.JSONObject;

import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.hierarchical.Field;
import de.tuberlin.dima.presslufthammer.data.hierarchical.fields.BooleanField;
import de.tuberlin.dima.presslufthammer.data.hierarchical.fields.DoubleField;
import de.tuberlin.dima.presslufthammer.data.hierarchical.fields.FloatField;
import de.tuberlin.dima.presslufthammer.data.hierarchical.fields.Int32Field;
import de.tuberlin.dima.presslufthammer.data.hierarchical.fields.Int64Field;
import de.tuberlin.dima.presslufthammer.data.hierarchical.fields.RecordField;
import de.tuberlin.dima.presslufthammer.data.hierarchical.fields.StringField;

class JSONHelper {

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
            return new Int32Field(childSchema, (Integer) value);
        } else if (childSchema.getPrimitiveType() == PrimitiveType.BOOLEAN
                && value instanceof Boolean) {
            return new BooleanField(childSchema, (Boolean) value);
        } else if (childSchema.getPrimitiveType() == PrimitiveType.INT64
                && value instanceof Long) {
            return new Int64Field(childSchema, (Long) value);
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

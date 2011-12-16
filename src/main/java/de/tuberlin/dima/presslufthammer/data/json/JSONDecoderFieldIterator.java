package de.tuberlin.dima.presslufthammer.data.json;

import java.util.Iterator;
import java.util.Map.Entry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import de.tuberlin.dima.presslufthammer.data.Field;
import de.tuberlin.dima.presslufthammer.data.FieldIterator;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class JSONDecoderFieldIterator implements FieldIterator {
    private JSONDecoder decoder;
    private JSONObject jsonData;
    private SchemaNode schema;

    private Iterator<Entry<String, Object>> iterator;
    private Iterator<Object> arrayIterator = null;
    private String arrayKey = null;

    @SuppressWarnings("unchecked")
    public JSONDecoderFieldIterator(JSONDecoder decoder) {
        this.decoder = decoder;
        this.jsonData = this.decoder.getData();
        this.schema = this.decoder.getSchema();

        iterator = jsonData.entrySet().iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Field next() {
        Field field = null;
        while (arrayIterator != null && field == null) {
            if (arrayIterator.hasNext()) {
                Object value = arrayIterator.next();
                SchemaNode childSchema = schema.getField(arrayKey);
                field = JSONHelper.createField(value, childSchema);
            } else {
                arrayIterator = null;
                arrayKey = null;
            }
        }
        if (field != null) {
            return field;
        }

        while (iterator.hasNext() && field == null) {
            Entry<String, Object> entry = iterator.next();
            String key = entry.getKey();
            Object value = entry.getValue();

            if (schema.hasField(key)) {
                SchemaNode childSchema = schema.getField(key);
                if (childSchema.isRepeated()) {
                    if (value instanceof JSONArray) {
                        JSONArray array = (JSONArray) value;
                        arrayIterator = array.iterator();
                        arrayKey = key;
                        field = next();
                    }
                } else {
                    field = JSONHelper.createField(value, childSchema);
                }
            }
        }
        return field;
    }
}

package de.tuberlin.dima.presslufthammer.data.hierarchical.json;

import org.json.simple.JSONObject;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.hierarchical.FieldIterator;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordDecoder;

class JSONRecordDecoder implements RecordDecoder {
    private final SchemaNode schema;
    private final JSONObject jsonData;

    public JSONRecordDecoder(SchemaNode schema, JSONObject job) {
        this.schema = schema;
        jsonData = job;
    }

    public RecordDecoder newDecoder(SchemaNode schema, Object data) {
        JSONObject job = (JSONObject) data;
        return new JSONRecordDecoder(schema, job);
    }

    public JSONObject getData() {
        return jsonData;
    }
    
    public SchemaNode getSchema() {
        return schema;
    }

    @Override
    public FieldIterator fieldIterator() {
        return new JSONRecordDecoderFieldIterator(this);
    }
}

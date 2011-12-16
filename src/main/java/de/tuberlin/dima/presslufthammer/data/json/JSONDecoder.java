package de.tuberlin.dima.presslufthammer.data.json;

import org.json.simple.JSONObject;

import de.tuberlin.dima.presslufthammer.data.FieldIterator;
import de.tuberlin.dima.presslufthammer.data.RecordDecoder;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

class JSONDecoder implements RecordDecoder {
    private final SchemaNode schema;
    private final JSONObject jsonData;

    public JSONDecoder(SchemaNode schema, JSONObject job) {
        this.schema = schema;
        jsonData = job;
    }

    public RecordDecoder newDecoder(SchemaNode schema, Object data) {
        JSONObject job = (JSONObject) data;
        return new JSONDecoder(schema, job);
    }

    public JSONObject getData() {
        return jsonData;
    }
    
    public SchemaNode getSchema() {
        return schema;
    }

    @Override
    public FieldIterator fieldIterator() {
        return new JSONDecoderFieldIterator(this);
    }
}

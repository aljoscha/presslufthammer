package de.tuberlin.dima.presslufthammer.data.hierarchical.json;

import java.io.IOException;
import java.util.List;
import java.util.Stack;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordEncoder;

public class JSONRecordEncoder implements RecordEncoder {
    private SchemaNode schema;
    private JSONObject rootJob;
    private Stack<JSONObject> jobStack;
    private Stack<SchemaNode> schemaStack;
    private JSONRecordFile file;

    public JSONRecordEncoder(SchemaNode schema, JSONRecordFile file) {
        this.schema = schema;
        this.file = file;
        rootJob = new JSONObject();
        jobStack = new Stack<JSONObject>();
        jobStack.push(rootJob);
        schemaStack = new Stack<SchemaNode>();
        schemaStack.push(this.schema);
    }

    @SuppressWarnings("unchecked")
    public void appendValue(SchemaNode childSchema, Object value) {
        SchemaNode currentSchema = schemaStack.peek();
        JSONObject currentJob = jobStack.peek();

        if (currentSchema.hasField(childSchema.getName())) {
            if (childSchema.isRepeated()) {
                // create a new JSONArray or append to the exisiting one:
                JSONArray array = null;
                if (currentJob.containsKey(childSchema.getName())) {
                    array = (JSONArray) currentJob.get(childSchema.getName());
                } else {
                    array = new JSONArray();
                    currentJob.put(childSchema.getName(), array);
                }
                array.add(value);
            } else {
                currentJob.put(childSchema.getName(), value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void moveToLevel(SchemaNode schema) {
        SchemaNode currentSchema = schemaStack.peek();
        JSONObject currentJob = jobStack.peek();
        List<SchemaNode> path = schema.getPath();
        int currentPositionInPath = path.indexOf(currentSchema);

        for (int i = currentPositionInPath + 1; i < path.size(); ++i) {
            SchemaNode newSchema = path.get(i);
            schemaStack.push(newSchema);

            JSONObject newJob = new JSONObject();
            if (newSchema.isRepeated()) {
                // create a new JSONArray or append to the exisiting one:
                JSONArray array = null;
                if (currentJob.containsKey(newSchema.getName())) {
                    array = (JSONArray) currentJob.get(newSchema.getName());
                } else {
                    array = new JSONArray();
                    currentJob.put(newSchema.getName(), array);
                }
                array.add(newJob);
            } else {
                currentJob.put(newSchema.getName(), newJob);
            }

            jobStack.push(newJob);
            currentJob = newJob;
        }
    }

    public void returnToLevel(SchemaNode schema) {
        SchemaNode currentSchema = schemaStack.peek();
        List<SchemaNode> path = currentSchema.getPath();
        int newPositionInPath = path.indexOf(schema);
        if (newPositionInPath == -1) {
            // schema is not in the current path, so it is probably further down
            // the tree
            return;
        }

        for (int i = path.size() - 1; i > newPositionInPath; --i) {
            schemaStack.pop();
            if (schemaStack.size() < 1) {
                throw new RuntimeException("Should not happen... level: "
                        + schema + " current schema: "
                        + currentSchema.getQualifiedName());
            }
            jobStack.pop();
        }
    }

    public JSONObject getJob() {
        return rootJob;
    }

    public void finalizeRecord() {
        System.out.println("NEW JOB:");
        System.out.println(rootJob.toJSONString());
        try {
            file.writeRecord(this);
        } catch (IOException e) {
            // oops TODO handle this somehow
            e.printStackTrace();
        }
    }
}

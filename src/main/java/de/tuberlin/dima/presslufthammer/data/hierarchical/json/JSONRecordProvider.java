package de.tuberlin.dima.presslufthammer.data.hierarchical.json;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordDecoder;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordProvider;

public class JSONRecordProvider implements RecordProvider {
    private Scanner scan;
    private SchemaNode schema;

    public JSONRecordProvider(SchemaNode schema, String filename)
            throws FileNotFoundException, IOException {
        this.schema = schema;

        scan = new Scanner(new File(filename));
    }

    public RecordDecoder next() {
        if (scan.hasNextLine()) {
            JSONObject job = (JSONObject) JSONValue.parse(scan.nextLine());
            RecordDecoder decoder = new JSONRecordDecoder(schema, job);
            return decoder;
        } else {
            return null;
        }
    }
}

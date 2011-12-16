package de.tuberlin.dima.presslufthammer.data.json;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.util.Scanner;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import de.tuberlin.dima.presslufthammer.data.RecordDecoder;
import de.tuberlin.dima.presslufthammer.data.RecordProvider;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class JSONRecordProvider implements RecordProvider {
    private Scanner scan;
    private SchemaNode schema;

    public JSONRecordProvider(SchemaNode schema, URL filename)
            throws FileNotFoundException, IOException {
        this.schema = schema;

        try {

            scan = new Scanner(new StringReader(Resources.toString(filename,
                    Charsets.UTF_8)));

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public RecordDecoder next() {
        if (scan.hasNextLine()) {
            JSONObject job = (JSONObject) JSONValue.parse(scan.nextLine());
            RecordDecoder decoder = new JSONDecoder(schema, job);
            return decoder;
        } else {
            return null;
        }
    }
}

package de.tuberlin.dima.presslufthammer.util;

import java.io.File;
import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.FieldStriper;
import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.columnar.local.LocalDiskDataStore;
import de.tuberlin.dima.presslufthammer.data.hierarchical.json.JSONRecordFile;

public class Columnarizer {

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("json-file schema-file data-dir partition-num");
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            printUsage();
            return;
        }

        String jsonFile = args[0];
        String schemaFile = args[1];
        String dataDir = args[2];
        int partitionNum = Integer.parseInt(args[3]);

        File directory = new File(dataDir);
        LocalDiskDataStore dataStore = null;
        if (!directory.exists()) {
            dataStore = LocalDiskDataStore.createDataStore(directory);
        } else {
            dataStore = LocalDiskDataStore.openDataStore(directory);
        }

        SchemaNode schema = ProtobufSchemaHelper.readSchemaFromFile(schemaFile);

        JSONRecordFile jsonRecords = new JSONRecordFile(schema, jsonFile);

        Tablet tablet = dataStore.createOrGetTablet(schema, partitionNum);

        FieldStriper striper = new FieldStriper(schema);
        
        striper.dissectRecords(jsonRecords, tablet);
        
        dataStore.flush();
    }

}

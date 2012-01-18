package de.tuberlin.dima.presslufthammer.util;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.BasicConfigurator;

import de.tuberlin.dima.presslufthammer.data.FieldStriper;
import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.hierarchical.json.JSONRecordFile;
import de.tuberlin.dima.presslufthammer.data.ondisk.OnDiskDataStore;

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
        BasicConfigurator.configure();
        if (args.length < 4) {
            printUsage();
            return;
        }

        String jsonFile = args[0];
        String schemaFile = args[1];
        String dataDir = args[2];
        int partitionNum = Integer.parseInt(args[3]);

        File directory = new File(dataDir);
        OnDiskDataStore dataStore = null;
        if (!directory.exists()) {
            dataStore = OnDiskDataStore.createDataStore(directory);
        } else {
            dataStore = OnDiskDataStore.openDataStore(directory);
        }

        SchemaNode schema = ProtobufSchemaHelper.readSchemaFromFile(schemaFile);

        JSONRecordFile jsonRecords = new JSONRecordFile(schema, jsonFile);

        Tablet tablet = dataStore.getTablet(schema, partitionNum);

        FieldStriper striper = new FieldStriper(schema);
        
        striper.dissectRecords(jsonRecords, tablet);
        
        dataStore.flush();
    }

}
package de.tuberlin.dima.presslufthammer.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

/**
 * This loads a config from a .json file and provide access to the configuration
 * values.
 * 
 * <p>
 * This has no error checking whatsoever, as most of the stuff in this project
 * for now ... :D
 * 
 * @author Aljoscha Krettek
 * 
 */
public class Config {
    Map<String, TableConfig> tables;

    public Config(File configFile) throws FileNotFoundException {
        tables = Maps.newHashMap();

        BufferedReader reader = new BufferedReader(new FileReader(configFile));
        JSONObject jsonConfig = (JSONObject) JSONValue.parse(reader);

        JSONArray jsonTables = (JSONArray) jsonConfig.get("tables");

        for (Object job : jsonTables) {
            JSONObject jsonTable = (JSONObject) job;
            String tableName = (String) jsonTable.get("name");
            String schemaPath = (String) jsonTable.get("schema");
            Long numPartitions = (Long) jsonTable.get("partitions");
            File schemaFile = new File(configFile.getParentFile(), schemaPath);
            tables.put(tableName, new TableConfig(schemaFile, numPartitions));
        }
    }

    public Map<String, TableConfig> getTables() {
        return tables;
    }

    public static class TableConfig {
        private final SchemaNode schema;
        private final long numPartitions;

        public TableConfig(File schemaFile, long numPartitions) {
            this.schema = ProtobufSchemaHelper.readSchemaFromFile(schemaFile
                    .getPath());
            this.numPartitions = numPartitions;
        }

        public SchemaNode getSchema() {
            return schema;
        }

        public long getNumPartitions() {
            return numPartitions;
        }
    }
}

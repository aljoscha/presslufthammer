package de.tuberlin.dima.presslufthammer.xml;

import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;


public class DataSource {
	private final int numPartitions;
	private final String schemapath;
	private final SchemaNode schema;
	
	public DataSource(String schemaPath, int parts) {
		this.numPartitions = parts;
		this.schemapath = schemaPath;
		this.schema = ProtobufSchemaHelper.readSchemaFromFile(schemaPath);
	}

	@Override
	public String toString() {
		String string = "DataSource: " + schema.getName() + "@" + schemapath;
		return string;
	}

	public SchemaNode getSchema() {
		return schema;
	}

	public int getNumPartitions() {
		return numPartitions;
	}
}

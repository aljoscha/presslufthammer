package de.tuberlin.dima.presslufthammer.xml;

import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;


public class DataSource {
	private final int partitions;
	private final String name;
	private final String schemapath;
	private final SchemaNode schema;
	
	public DataSource(String name, String path, int parts) {
		this.partitions = parts;
		this.name = name;
		this.schemapath = path;
		this.schema = ProtobufSchemaHelper.readSchema(path, name);
	}

	@Override
	public String toString() {
		String string = "DataSource: " + name + "@" + schemapath;
		return string;
	}

	public SchemaNode getSchema() {
		return schema;
	}

	public int getPartitions() {
		return partitions;
	}
}

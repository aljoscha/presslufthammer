package de.tuberlin.dima.presslufthammer.xml;

import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;


/**
 * @author feichh
 *
 */
public class DataSource {
	private final int numPartitions;
	private final String schemapath;
	private final SchemaNode schema;
	
	/**
	 * @param schemaPath
	 * @param parts
	 */
	public DataSource(String schemaPath, int parts) {
		this.numPartitions = parts;
		this.schemapath = schemaPath;
		this.schema = ProtobufSchemaHelper.readSchemaFromFile(schemaPath);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String string = "DataSource: " + schema.getName() + "@" + schemapath + "Partitions: " + numPartitions;
		return string;
	}

	/**
	 * @return
	 */
	public SchemaNode getSchema() {
		return schema;
	}

	/**
	 * @return
	 */
	public int getNumPartitions() {
		return numPartitions;
	}
}

package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;

public class InMemoryReadonlyTablet implements Tablet {
    private SchemaNode schema;
    private Map<SchemaNode, byte[]> columns;
    private Map<SchemaNode, ColumnReaderImpl> columnReaders;

    public InMemoryReadonlyTablet(SchemaNode schema,
            Map<SchemaNode, byte[]> columns) {
        this.schema = schema;
        this.columns = columns;
        columnReaders = Maps.newHashMap();
        createColumnReaders(schema);
    }

    public InMemoryReadonlyTablet(InMemoryWriteonlyTablet sourceTablet) {
        this(sourceTablet.getSchema(), sourceTablet.getColumnData());
    }

    public InMemoryReadonlyTablet(byte[] data) {
        ByteArrayInputStream inArray = new ByteArrayInputStream(data);
        DataInputStream in = new DataInputStream(inArray);
        Map<String, byte[]> rawColumns = Maps.newHashMap();

        try {
            int magicNumber = in.readInt();
            if (magicNumber != InMemoryWriteonlyTablet.PRESSLUFT_TABLET_MAGIC_NUMBER) {
                // this should not happen, programming bug
                throw new RuntimeException(
                        "Pressluft handed to InMemoryReadonlyTablet with wrong magic number.");
            }

            String schemaString = in.readUTF();
            this.schema = ProtobufSchemaHelper
                    .readSchemaFromString(schemaString);

            int numColumns = in.readInt();

            for (int i = 0; i < numColumns; ++i) {
                String qualifiedColumnName = in.readUTF();
                int numBytes = in.readInt();
                byte[] buffer = new byte[numBytes];
                if (numBytes > 0) {
                    int bytesRead = in.read(buffer, 0, numBytes);
                    if (bytesRead != numBytes) {
                        throw new RuntimeException(
                                "Mismatch in number of bytes expected and actually read.\nField: "
                                        + qualifiedColumnName + " " + bytesRead
                                        + "/" + numBytes);
                    }
                }
                rawColumns.put(qualifiedColumnName, buffer);
            }

        } catch (IOException e) {
            // cannot happen
            e.printStackTrace();
        }
        columns = Maps.newHashMap();
        columnReaders = Maps.newHashMap();
        fillColumns(schema, rawColumns);
        createColumnReaders(schema);
    }

    private void fillColumns(SchemaNode schema, Map<String, byte[]> rawColumns) {
        columns.put(schema, rawColumns.get(schema.getQualifiedName()));

        if (schema.isRecord()) {
            for (SchemaNode childSchema : schema.getFieldList()) {
                fillColumns(childSchema, rawColumns);
            }
        }
    }

    private void createColumnReaders(SchemaNode schema) {
        ByteArrayInputStream arrayStream = new ByteArrayInputStream(
                columns.get(schema));
        DataInputStream in = new DataInputStream(new BufferedInputStream(
                arrayStream));
        try {
            ColumnReaderImpl reader = new ColumnReaderImpl(schema, in);
            columnReaders.put(schema, reader);
        } catch (IOException e) {
            // This cannot happen, we do nothing with I/O here, comes from the
            // output stream shenanigans inside InMemoryColumnReader
            e.printStackTrace();
        }

        if (schema.isRecord()) {
            for (SchemaNode childSchema : schema.getFieldList()) {
                createColumnReaders(childSchema);
            }
        }
    }

    public SchemaNode getSchema() {
        return schema;
    }

    public boolean hasColumn(SchemaNode schema) {
        return columns.containsKey(schema);
    }

    public ColumnWriter getColumnWriter(SchemaNode schema) {
        throw new RuntimeException(
                "This is a read-only tablet, call to this should not happen. (getColumnWriter)");
    }

    public ColumnReader getColumnReader(SchemaNode schema) {
        if (!columns.containsKey(schema)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        return columnReaders.get(schema);
    }

    public void printColumns() {
        for (SchemaNode schema : columns.keySet()) {
            System.out.println("COLUMN: " + schema.getQualifiedName());
            System.out.println("SIZE: " + columns.get(schema).length);
            ByteArrayInputStream arrayStream = new ByteArrayInputStream(
                    columns.get(schema));
            DataInputStream in = new DataInputStream(new BufferedInputStream(
                    arrayStream));
            try {
                ColumnReaderImpl reader = new ColumnReaderImpl(schema, in);
                while (reader.hasNext()) {
                    System.out.println(reader.getNextRepetition());
                    System.out.println(reader.getNextDefinition());
                    System.out.println(reader.getNextValue());
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}

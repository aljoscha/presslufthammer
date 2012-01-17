package de.tuberlin.dima.presslufthammer.data.columnar;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;
import de.tuberlin.dima.presslufthammer.pressluft.Type;

public class InMemoryWriteonlyTablet implements Tablet {
    public static int PRESSLUFT_TABLET_MAGIC_NUMBER = 0xCAFEBABE;
    private SchemaNode schema;
    private Map<SchemaNode, ByteArrayOutputStream> columns;
    private Map<SchemaNode, ColumnWriterImpl> columnWriters;

    public InMemoryWriteonlyTablet(SchemaNode schema) {
        this.schema = schema;
        columns = Maps.newHashMap();
        columnWriters = Maps.newHashMap();
        createColumns(schema);
    }

    public Pressluft toPressluft() {
        try {
            flush();
        } catch (IOException e) {
            // Should not happen
            e.printStackTrace();
        }
        ByteArrayOutputStream arrayOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(arrayOut);

        try {
            out.writeInt(PRESSLUFT_TABLET_MAGIC_NUMBER);
            out.writeUTF(schema.toString());
            out.writeInt(columns.size());
            for (SchemaNode schema : columns.keySet()) {
                out.writeUTF(schema.getQualifiedName());
                byte[] columnData = columns.get(schema).toByteArray();
                out.writeInt(columnData.length);
                out.write(columnData, 0, columnData.length);
            }
            out.flush();
        } catch (IOException e1) {
            // cannot happen
        }

        return new Pressluft(Type.RESULT, (byte) -1, arrayOut.toByteArray());
    }

    private void createColumns(SchemaNode schema) {
        ByteArrayOutputStream column = new ByteArrayOutputStream();
        try {
            ColumnWriterImpl writer = new ColumnWriterImpl(schema, column);
            columnWriters.put(schema, writer);
        } catch (IOException e) {
            // This cannot happen, we do nothing with I/O here, comes from the
            // input stream shenanigans inside InMemoryColumnWriter
            e.printStackTrace();
        }

        columns.put(schema, column);
        if (schema.isRecord()) {
            for (SchemaNode childSchema : schema.getFieldList()) {
                createColumns(childSchema);
            }
        }
    }

    public Map<SchemaNode, byte[]> getColumnData() {
        Map<SchemaNode, byte[]> result = Maps.newHashMap();

        for (SchemaNode key : columns.keySet()) {
            result.put(key, columns.get(key).toByteArray());
        }

        return result;
    }

    public SchemaNode getSchema() {
        return schema;
    }

    public boolean hasColumn(SchemaNode schema) {
        return columns.containsKey(schema);
    }

    public ColumnWriter getColumnWriter(SchemaNode schema) {
        if (!columns.containsKey(schema)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        return columnWriters.get(schema);
    }

    public ColumnReader getColumnReader(SchemaNode schema) {
        throw new RuntimeException(
                "This is a write-only tablet, call to this should not happen. (getColumnReader)");
    }

    public void flush() throws IOException {
        for (SchemaNode schema : columns.keySet()) {
            columnWriters.get(schema).flush();
        }
    }

    public void printColumns() {
        try {
            flush();
        } catch (IOException e1) {
            System.out.println("Error printing the columns:");
            e1.printStackTrace();
        }
        for (SchemaNode schema : columns.keySet()) {
            System.out.println("COLUMN: " + schema.getQualifiedName());
            System.out.println("SIZE: "
                    + columns.get(schema).toByteArray().length);
            ByteArrayInputStream arrayStream = new ByteArrayInputStream(columns
                    .get(schema).toByteArray());
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

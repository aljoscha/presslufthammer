package de.tuberlin.dima.presslufthammer.data.columnar.ondisk;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.data.ProtobufSchemaHelper;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReaderImpl;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriterImpl;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;

public class OnDiskTablet implements Tablet {
    private SchemaNode schema;
    private File directory;
    private Map<SchemaNode, File> columnFiles;
    private Map<SchemaNode, ColumnWriterImpl> columnWriters;

    private OnDiskTablet(SchemaNode schema, File directory) {
        this.schema = schema;
        this.directory = directory;
        columnWriters = Maps.newHashMap();
        columnFiles = Maps.newHashMap();
    }

    public static OnDiskTablet openTablet(File directory, String messageName)
            throws IOException {
        if (!directory.exists() && directory.isDirectory()) {
            throw new IOException(
                    "Directory given in openTablet does not exist or is not a directory.");
        }
        File schemaFile = new File(directory, "schema.proto");
        if (!schemaFile.exists()) {
            throw new IOException("No schema file in tablet directory "
                    + directory);
        }
        SchemaNode schema = ProtobufSchemaHelper.readSchema(
                schemaFile.getAbsolutePath(), messageName);
        OnDiskTablet tablet = new OnDiskTablet(schema, directory);
        tablet.createOrOpenColumnFiles(directory, schema);

        return tablet;
    }

    public static void removeTablet(File directory) throws IOException {
        if (directory.exists()) {
            FileUtils.deleteDirectory(directory);
        }
    }

    public static OnDiskTablet createTablet(SchemaNode schema, File directory)
            throws IOException {
        OnDiskTablet tablet = new OnDiskTablet(schema, directory);
        if (directory.exists()) {
            throw new RuntimeException("Directory for tablet already exists: "
                    + directory);
        }
        directory.mkdirs();
        tablet.createOrOpenColumnFiles(directory, schema);

        File schemaFile = new File(directory, "schema.proto");
        PrintWriter schemaWriter = new PrintWriter(new FileWriter(schemaFile));
        schemaWriter.write(schema.toString());
        schemaWriter.close();
        return tablet;
    }

    private void createOrOpenColumnFiles(File directory, SchemaNode schema)
            throws IOException {
        if (schema.isRecord()) {
            for (SchemaNode childSchema : schema.getFieldList()) {
                createOrOpenColumnFiles(directory, childSchema);
            }
        } else {
            File columnFile = new File(directory, schema.getQualifiedName()
                    + ".column");
            columnFile.createNewFile();

            columnWriters.put(schema, new ColumnWriterImpl(schema,
                    new FileOutputStream(columnFile, true)));
            // We need these for creating the readers
            columnFiles.put(schema, columnFile);
        }
    }

    public File getDirectory() {
        return directory;
    }

    @Override
    public SchemaNode getSchema() {
        return schema;
    }

    @Override
    public ColumnWriter getColumnWriter(SchemaNode schema) {
        if (schema.isRecord()) {
            // return a dummy writer so that the FieldStriper is happy,
            // he creates FieldWriters for records also ...
            return new DummyColumnWriter();
        }
        if (!columnWriters.containsKey(schema)) {
            System.out.println("OnDisk column writer requested but not there: "
                    + schema.getQualifiedName());
            System.out.println("Available columns:");
            for (SchemaNode avSchema : columnWriters.keySet()) {
                System.out.println(avSchema.getQualifiedName());
            }
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        return columnWriters.get(schema);
    }

    @Override
    public ColumnReader getColumnReader(SchemaNode schema) {
        if (schema.isRecord()) {
            return null;
        }
        if (!columnFiles.containsKey(schema)) {
            System.out.println("OnDisk column reader requested but not there: "
                    + schema.getQualifiedName());
            System.out.println("Available columns:");
            for (SchemaNode avSchema : columnWriters.keySet()) {
                System.out.println(avSchema.getQualifiedName());
            }
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }

        try {
            columnWriters.get(schema).flush();
            return new ColumnReaderImpl(schema, new FileInputStream(
                    columnFiles.get(schema)));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void close() throws IOException {
        for (ColumnWriterImpl writer : columnWriters.values()) {
            writer.close();
        }
    }

    public void flush() throws IOException {
        for (ColumnWriterImpl writer : columnWriters.values()) {
            writer.flush();
        }
    }
}

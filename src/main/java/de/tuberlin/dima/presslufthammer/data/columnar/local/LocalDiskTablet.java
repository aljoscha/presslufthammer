package de.tuberlin.dima.presslufthammer.data.columnar.local;

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
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReaderBool;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReaderDouble;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReaderFloat;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReaderInt32;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReaderInt64;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReaderString;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;

/**
 * {@link Tablet} implementation that stores the column data in files on a local
 * disk. The data is stored in one directory on disk, this directory has one
 * file called "schema.proto" that contains the schema of the tablet and one
 * binary file per column that stores the columnar data (at the beginning this
 * file might be empty).
 * 
 * @author Aljoscha Krettek
 * 
 */
public class LocalDiskTablet implements Tablet {
    private SchemaNode schema;
    private Map<SchemaNode, File> columnFiles;
    private Map<SchemaNode, ColumnWriter> columnWriters;

    /**
     * Internal constructor.
     */
    private LocalDiskTablet(SchemaNode schema, File directory) {
        this.schema = schema;
        columnWriters = Maps.newHashMap();
        columnFiles = Maps.newHashMap();
    }

    /**
     * Opens an existing tablet from the specified directory.
     */
    public static LocalDiskTablet openTablet(File directory) throws IOException {
        if (!directory.exists() && directory.isDirectory()) {
            throw new IOException(
                    "Directory given in openTablet does not exist or is not a directory.");
        }
        File schemaFile = new File(directory, "schema.proto");
        if (!schemaFile.exists()) {
            throw new IOException("No schema file in tablet directory "
                    + directory);
        }
        SchemaNode schema = ProtobufSchemaHelper.readSchemaFromFile(schemaFile
                .getAbsolutePath());
        LocalDiskTablet tablet = new LocalDiskTablet(schema, directory);
        tablet.createOrOpenColumnFiles(directory, schema);

        return tablet;
    }

    /**
     * Creates a new tablet in the given directory for the given schema.
     */
    public static LocalDiskTablet createTablet(SchemaNode schema, File directory)
            throws IOException {
        LocalDiskTablet tablet = new LocalDiskTablet(schema, directory);
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

    /**
     * Removes the tablet, ie. removes the directory.
     */
    public static void removeTablet(File directory) throws IOException {
        if (directory.exists()) {
            FileUtils.deleteDirectory(directory);
        }
    }

    /**
     * Internal methods that creates or opens the files that contain the column
     * data. The internal map of ColumnReaderImpls is also filled here.
     */
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

            columnWriters.put(schema, new ColumnWriter(schema,
                    new FileOutputStream(columnFile, true)));
            // We need these for creating the readers
            columnFiles.put(schema, columnFile);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaNode getSchema() {
        return schema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasColumn(SchemaNode schema) {
        return columnFiles.containsKey(schema);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ColumnWriter getColumnWriter(SchemaNode schema) {
        if (schema.isRecord()) {
            throw new RuntimeException(
                    "Should not happen: getColumnWriter called for schema that is RECORD.");
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

    /**
     * {@inheritDoc}
     */
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
            switch (schema.getPrimitiveType()) {
            case INT32:
                return new ColumnReaderInt32(schema, new FileInputStream(
                        columnFiles.get(schema)));
            case INT64:
                return new ColumnReaderInt64(schema, new FileInputStream(
                        columnFiles.get(schema)));
            case BOOLEAN:
                return new ColumnReaderBool(schema, new FileInputStream(
                        columnFiles.get(schema)));
            case FLOAT:
                return new ColumnReaderFloat(schema, new FileInputStream(
                        columnFiles.get(schema)));
            case DOUBLE:
                return new ColumnReaderDouble(schema, new FileInputStream(
                        columnFiles.get(schema)));
            case STRING:
                return new ColumnReaderString(schema, new FileInputStream(
                        columnFiles.get(schema)));
            default:
                throw new RuntimeException("Unknown primitive type.");
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Calls {@code close} on all column writers. Should be called on shutdown.
     */
    public void close() throws IOException {
        for (ColumnWriter writer : columnWriters.values()) {
            writer.close();
        }
    }

    /**
     * Calls {@code flush} on all column writers. Should be called before
     * reading from the tablet columns.
     */
    public void flush() throws IOException {
        for (ColumnWriter writer : columnWriters.values()) {
            writer.flush();
        }
    }
}

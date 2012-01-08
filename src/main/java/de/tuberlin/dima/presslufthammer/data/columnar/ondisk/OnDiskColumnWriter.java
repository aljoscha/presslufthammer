package de.tuberlin.dima.presslufthammer.data.columnar.ondisk;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.hierarchical.Field;

public class OnDiskColumnWriter implements ColumnWriter {
    private SchemaNode schema;
    private File file;
    private PrintWriter writer;

    protected OnDiskColumnWriter(SchemaNode schema, File file)
            throws IOException {
        this.schema = schema;
        this.file = file;
        writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
    }

    public void close() {
        writer.close();
    }
    
    public void flush() {
        writer.flush();
    }
    
    public File getFile() {
        return file;
    }

    public void writeField(Field field, int repetitionLevel, int definitionLevel) {
        field.writeToColumn(this, repetitionLevel, definitionLevel);
    }

    @Override
    public void writeInt32(int value, int repetitionLevel, int definitionLevel) {
        if (!schema.getPrimitiveType().equals(PrimitiveType.INT32)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        writer.write(Integer.toString(repetitionLevel));
        writer.write("\n");
        writer.write(Integer.toString(definitionLevel));
        writer.write("\n");
        writer.write(Integer.toString(value));
        writer.write("\n");
    }

    @Override
    public void writeInt64(long value, int repetitionLevel, int definitionLevel) {
        if (!schema.getPrimitiveType().equals(PrimitiveType.INT64)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        writer.write(Integer.toString(repetitionLevel));
        writer.write("\n");
        writer.write(Integer.toString(definitionLevel));
        writer.write("\n");
        writer.write(Long.toString(value));
        writer.write("\n");
    }

    @Override
    public void writeBool(boolean value, int repetitionLevel,
            int definitionLevel) {
        if (!schema.getPrimitiveType().equals(PrimitiveType.BOOLEAN)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        writer.write(Integer.toString(repetitionLevel));
        writer.write("\n");
        writer.write(Integer.toString(definitionLevel));
        writer.write("\n");
        writer.write(Boolean.toString(value));
        writer.write("\n");
    }

    @Override
    public void writeFloat(float value, int repetitionLevel, int definitionLevel) {
        if (!schema.getPrimitiveType().equals(PrimitiveType.FLOAT)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        writer.write(Integer.toString(repetitionLevel));
        writer.write("\n");
        writer.write(Integer.toString(definitionLevel));
        writer.write("\n");
        writer.write(Float.toString(value));
        writer.write("\n");
    }

    @Override
    public void writeDouble(double value, int repetitionLevel,
            int definitionLevel) {
        if (!schema.getPrimitiveType().equals(PrimitiveType.DOUBLE)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        writer.write(Integer.toString(repetitionLevel));
        writer.write("\n");
        writer.write(Integer.toString(definitionLevel));
        writer.write("\n");
        writer.write(Double.toString(value));
        writer.write("\n");
    }

    @Override
    public void writeString(String value, int repetitionLevel,
            int definitionLevel) {
        if (!schema.getPrimitiveType().equals(PrimitiveType.STRING)) {
            throw new RuntimeException(
                    "This should not happen, bug in program.");
        }
        writer.write(Integer.toString(repetitionLevel));
        writer.write("\n");
        writer.write(Integer.toString(definitionLevel));
        writer.write("\n");
        writer.write(value);
        writer.write("\n");
    }

    @Override
    public void writeNull(int repetitionLevel, int definitionLevel) {
        writer.write(Integer.toString(repetitionLevel));
        writer.write("\n");
        writer.write(Integer.toString(definitionLevel));
        writer.write("\n");
    }
}

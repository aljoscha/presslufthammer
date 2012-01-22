package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;

public class TabletProjector {

    public void project(SchemaNode schema, Tablet src, Tablet dest)
            throws IOException {
        projectColumns(schema, src, dest);
    }

    private void projectColumns(SchemaNode schema, Tablet src, Tablet dest)
            throws IOException {

        if (schema.isRecord()) {
            for (SchemaNode childSchema : schema.getFieldList()) {
                projectColumns(childSchema, src, dest);
            }
        } else {
            ColumnReader reader = src.getColumnReader(schema);
            ColumnWriter writer = dest.getColumnWriter(schema);

            while (reader.hasNext()) {
                int def = reader.getNextDefinition();
                int rep = reader.getNextRepetition();

                switch (schema.getPrimitiveType()) {
                case INT32: {
                    if (reader.nextIsNull()) {
                        reader.getNextValue();
                        writer.writeNull(rep, def);
                    } else {
                        int value = reader.getNextInt32();
                        writer.writeInt32(value, rep, def);
                    }
                    break;
                }
                case INT64: {
                    if (reader.nextIsNull()) {
                        reader.getNextValue();
                        writer.writeNull(rep, def);
                    } else {
                        long value = reader.getNextInt64();
                        writer.writeInt64(value, rep, def);
                    }
                    break;
                }
                case FLOAT: {
                    if (reader.nextIsNull()) {
                        reader.getNextValue();
                        writer.writeNull(rep, def);
                    } else {
                        float value = reader.getNextFloat();
                        writer.writeFloat(value, rep, def);
                    }
                    break;
                }
                case DOUBLE: {
                    if (reader.nextIsNull()) {
                        reader.getNextValue();
                        writer.writeNull(rep, def);
                    } else {
                        double value = reader.getNextDouble();
                        writer.writeDouble(value, rep, def);
                    }
                    break;
                }
                case BOOLEAN: {
                    if (reader.nextIsNull()) {
                        reader.getNextValue();
                        writer.writeNull(rep, def);
                    } else {
                        boolean value = reader.getNextBool();
                        writer.writeBool(value, rep, def);
                    }
                    break;
                }
                case STRING: {
                    if (reader.nextIsNull()) {
                        reader.getNextValue();
                        writer.writeNull(rep, def);
                    } else {
                        String value = reader.getNextString();
                        writer.writeString(value, rep, def);
                    }
                    break;
                }
                }
            }
        }
    }
}

package de.tuberlin.dima.presslufthammer.data;

/**
 * Enum representing the possible types of primitive fields in a schema (
 * {@link SchemaNode}).
 * 
 * <p>
 * The toString() methods returns the name of the type that is compatible with
 * the primitive types in a .proto file.
 * 
 * @author Aljoscha Krettek
 * 
 */
public enum PrimitiveType {
    INT32("int32"), INT64("int64"), BOOLEAN("bool"), FLOAT("float"), DOUBLE(
            "double"), STRING("string");

    private String name;

    private PrimitiveType(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }
}
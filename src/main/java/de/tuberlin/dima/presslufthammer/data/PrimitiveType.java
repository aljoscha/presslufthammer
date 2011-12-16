package de.tuberlin.dima.presslufthammer.data;

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
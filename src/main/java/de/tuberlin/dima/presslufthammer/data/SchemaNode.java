package de.tuberlin.dima.presslufthammer.data;

import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class SchemaNode {
    public enum NodeType {
        RECORD, PRIMITIVE
    }

    public enum FieldRule {
        REQUIRED, OPTIONAL, REPEATED
    }

    // only valid for records (groups)
    private List<SchemaNode> fieldList = null;
    private Map<String, SchemaNode> fieldMap = null;

    // valid if type is primitive
    private PrimitiveType primitiveType = null;

    private String name;
    private NodeType type;

    private FieldRule fieldRule = FieldRule.REQUIRED;

    private SchemaNode parent = null;

    private SchemaNode(NodeType nodeType) {
        type = nodeType;
    }

    public static SchemaNode createPrimitive(String name, PrimitiveType type) {
        SchemaNode newSchema = new SchemaNode(NodeType.PRIMITIVE);
        newSchema.name = name;
        newSchema.primitiveType = type;
        return newSchema;
    }

    public static SchemaNode createRecord(String name) {
        SchemaNode newSchema = new SchemaNode(NodeType.RECORD);
        newSchema.name = name;
        newSchema.fieldList = Lists.newLinkedList();
        newSchema.fieldMap = Maps.newHashMap();
        return newSchema;
    }

    public String getName() {
        return name;
    }

    public NodeType getType() {
        return type;
    }

    public boolean isPrimitive() {
        return type == NodeType.PRIMITIVE;
    }

    public PrimitiveType getPrimitiveType() {
        if (type != NodeType.PRIMITIVE) {
            throw new RuntimeException("SchemaTree " + name
                    + " is not a primitive type.");
        }
        return primitiveType;
    }

    public boolean isRequired() {
        return fieldRule == FieldRule.REQUIRED;
    }

    public void setRequired() {
        fieldRule = FieldRule.REQUIRED;
    }

    public boolean isOptional() {
        return fieldRule == FieldRule.OPTIONAL;
    }

    public void setOptional() {
        fieldRule = FieldRule.OPTIONAL;
    }

    public boolean isRepeated() {
        return fieldRule == FieldRule.REPEATED;
    }

    public void setRepeated() {
        fieldRule = FieldRule.REPEATED;
    }

    public FieldRule getFieldRule() {
        return fieldRule;
    }

    public boolean isRecord() {
        return type == NodeType.RECORD;
    }

    public List<SchemaNode> getFieldList() {
        if (type != NodeType.RECORD) {
            throw new RuntimeException("SchemaTree " + name
                    + " is not a record type.");
        }
        return fieldList;
    }

    public void addField(SchemaNode newField) {
        addField(newField, newField.getName());
    }

    public void addField(SchemaNode newField, String name) {
        if (type != NodeType.RECORD) {
            throw new RuntimeException("Adding a field to SchemaTree " + name
                    + " is not possible because it is not a record.");
        }
        if (fieldMap.containsKey(name)) {
            throw new RuntimeException("Duplicate field name in SchemaTree "
                    + name + ".");
        }
        fieldList.add(newField);
        fieldMap.put(name, newField);
        newField.parent = this;
    }

    public boolean hasField(String fieldName) {
        return fieldMap.containsKey(fieldName);
    }

    public SchemaNode getField(String fieldName) {
        return fieldMap.get(fieldName);
    }

    public boolean hasParent() {
        return parent != null;
    }

    public SchemaNode getParent() {
        return parent;
    }

    public boolean equals(Object other) {
        if (!(other instanceof SchemaNode)) {
            return false;
        }

        SchemaNode otherSchema = (SchemaNode) other;

        if (getType() != otherSchema.getType()) {
            return false;
        }

        if (getFieldRule() != otherSchema.getFieldRule()) {
            return false;
        }

        if (isPrimitive()
                && !(this.primitiveType.equals(otherSchema.primitiveType))) {
            return false;
        }

        if (isRecord() && fieldList.size() != otherSchema.fieldList.size()) {
            return false;
        }

        if (isRecord()) {
            for (int i = 0; i < fieldList.size(); ++i) {
                if (!fieldList.get(i).equals(otherSchema.fieldList.get(i))) {
                    return false;
                }
            }
        }
        return true;
    }

    public int hashCode() {
        return Objects.hashCode(fieldMap, primitiveType, name, type, fieldRule);
    }

    public String getQualifiedName() {
        if (hasParent()) {
            return parent.getQualifiedName() + "." + name;
        } else {
            return name;
        }
    }

    public String toString() {
        return toStringRecursive(0);
    }

    private String toStringRecursive(int indentation) {
        StringBuffer identBuffer = new StringBuffer();
        for (int i = 0; i < indentation; i++) {
            identBuffer.append("      ");
        }
        indentation++;
        String ident = identBuffer.toString();

        String result = "";
        if (parent != null) {
            switch (fieldRule) {
            case REQUIRED:
                result += "required ";
                break;
            case OPTIONAL:
                result += "optional ";
                break;
            case REPEATED:
                result += "repeated ";
                break;
            }
        }
        switch (type) {
        case PRIMITIVE:
            result += primitiveType.toString() + " " + name + ";";
            break;
        case RECORD:
            if (parent == null) {
                result += "message ";
            } else {
                result += "group ";
            }
            result += name + " {\n";
            List<String> childStrings = Lists.newLinkedList();
            for (SchemaNode schema : fieldList) {
                childStrings.add(fieldMap.get(schema.getName())
                        .toStringRecursive(indentation));
            }
            Joiner join = Joiner.on('\n');
            result += join.join(childStrings);
            result += " }";
            break;
        default:
            throw new RuntimeException("Unexpected node type " + type);
        }

        return ident + result;
    }
}

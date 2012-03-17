package de.tuberlin.dima.presslufthammer.data;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Objects of this class are used to represent a hierarchical schema. This
 * representation can express most of the things that can be expressed in
 * protobuf format definitions (.proto files) and using
 * {@link ProtobufSchemaHelper} one can convert between the two. The schema is
 * expressed as a tree of SchemaNodeS, each schema but the root SchemaNode
 * therefore has a parent != null.
 * 
 * <p>
 * A SchemaNode can either represent a primitive field with a
 * {@link PrimitiveType} or a record field that contains zero or more other
 * fields (SchemaNodeS).
 * 
 * <p>
 * The name of a SchemaNode is the local name of that field, not a fully
 * qualified name. Though using {@code getQualifiedName} the qualified name can
 * be retrieved.
 * 
 * <p>
 * A SchemaNode can be one of required, optional or repeated. (check google
 * protobuf documentation).
 * 
 * @author Aljoscha Krettek
 * 
 */
public class SchemaNode {
    public enum Type {
        RECORD, PRIMITIVE
    }

    public enum Modifier {
        REQUIRED, OPTIONAL, REPEATED
    }

    // valid for all types (record, primitive)
    private String name;
    private Type type;
    private Modifier modifier;
    private SchemaNode parent = null;

    // only valid for records (groups)
    private List<SchemaNode> fieldList = null;
    private Map<String, SchemaNode> fieldMap = null;

    // valid if type is primitive
    private PrimitiveType primitiveType = null;

    /**
     * Constructs a schema of either primitive or record type and sets the
     * modified to required.
     */
    private SchemaNode(Type nodeType) {
        type = nodeType;
        this.modifier = Modifier.REQUIRED;
    }

    /**
     * Constructs a SchemaNode that representes a primitive field of the given
     * {@link PrimitiveType} and has the given name. Initially the field will be
     * "required".
     */
    public static SchemaNode createPrimitive(String name, PrimitiveType type) {
        SchemaNode newSchema = new SchemaNode(Type.PRIMITIVE);
        newSchema.name = name;
        newSchema.primitiveType = type;
        return newSchema;
    }

    /**
     * Constructs a SchemaNode that representes a record field and has the given
     * name. Initially the field will be "required" and the it will have no
     * child fields.
     */
    public static SchemaNode createRecord(String name) {
        SchemaNode newSchema = new SchemaNode(Type.RECORD);
        newSchema.name = name;
        newSchema.fieldList = Lists.newLinkedList();
        newSchema.fieldMap = Maps.newHashMap();
        return newSchema;
    }

    /**
     * Recursively calculates the "repetition" of the field represented by this
     * SchemaNode. (see dremel paper for definition of repetition and definition
     * levels)
     */
    public int getRepetition() {
        int parentRepetition = 0;
        if (this.parent != null) {
            parentRepetition = parent.getRepetition();
        }
        if (isRepeated()) {
            return parentRepetition + 1;
        } else {
            return parentRepetition;
        }

    }

    /**
     * Recursively calculates the "max definition" of the field represented by
     * this SchemaNode. (see dremel paper for definition of repetition and
     * definition levels)
     */
    public int getMaxDefinition() {
        int parentMaxDefinition = 0;
        if (this.parent != null) {
            parentMaxDefinition = parent.getMaxDefinition();
        }
        if (isOptional() || isRepeated()) {
            return parentMaxDefinition + 1;
        } else {
            return parentMaxDefinition;
        }
    }

    /**
     * Recursively calculates the "full definition" of the field represented by
     * this SchemaNode. (see dremel paper for definition of repetition and
     * definition levels)
     */
    public int getFullDefinition() {
        int parentFullDefinition = 0;
        if (this.parent != null) {
            parentFullDefinition = parent.getFullDefinition();
        }
        return parentFullDefinition + 1;
    }

    /**
     * Recursively constructs a {@link List} of SchemaNodeS that represents the
     * path that one has to take to reach this SchemaNode from the root record
     * of the schema.
     */
    public List<SchemaNode> getPath() {
        if (parent == null) {
            List<SchemaNode> result = Lists.newArrayList(this);
            return result;
        } else {
            List<SchemaNode> result = parent.getPath();
            result.add(this);
            return result;
        }
    }

    /**
     * Returns the "local" name of this schema.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the fully qualified name of this schema, it includes the names of
     * all parent schemas and looks like: foo.bar.baz.
     */
    public String getQualifiedName() {
        if (hasParent()) {
            return parent.getQualifiedName() + "." + name;
        } else {
            return name;
        }
    }

    /**
     * Returns the type of the field represented by this SchemaNode, either
     * RECORD or PRIMITIVE.
     */
    public Type getType() {
        return type;
    }

    /**
     * Returns true if this field (SchemaNode) is the first in the child list of
     * the parent SchemaNode or if the SchemaNode has no parent, i.e. it is the
     * root node of the schema.
     */
    public boolean isFirstField() {
        if (parent == null) {
            return true;
        } else {
            return parent.isFirstField(this);
        }
    }

    /**
     * Returns true when the given SchemaNode is the first in the list of child
     * schemas. Used internally by {@code isFirstChild}.
     */
    private boolean isFirstField(SchemaNode child) {
        return fieldList.indexOf(child) == 0;
    }

    /**
     * Returns true when this schema represents a primitive field.
     */
    public boolean isPrimitive() {
        return type == Type.PRIMITIVE;
    }

    /**
     * Returns true when this schema represents a record field.
     */
    public boolean isRecord() {
        return type == Type.RECORD;
    }

    /**
     * Returns the primitive type of this schema. Must only be called when the
     * field is a primitive field.
     */
    public PrimitiveType getPrimitiveType() {
        if (type != Type.PRIMITIVE) {
            throw new RuntimeException("SchemaTree " + name
                    + " is not a primitive type.");
        }
        return primitiveType;
    }

    /**
     * Sets the primitive type of this schema. Must only be caleld when the
     * field is a primitive field.
     * 
     * @param newType
     */
    public void setPrimitiveType(PrimitiveType newType) {
        if (type != Type.PRIMITIVE) {
            throw new RuntimeException("SchemaTree " + name
                    + " is not a primitive type.");
        }
        this.primitiveType = newType;
    }

    /**
     * Returns the SchemaNode in the path from schema root to this schema that
     * has a "max definition level" smaller or equal than the requested
     * definition level. This is used by the record assembly algorithm to adjust
     * the record structure when null fields are encountered.
     */
    public SchemaNode getSchemaForDefinitionLevel(int definitionLevel) {
        if (getMaxDefinition() <= definitionLevel) {
            return this;
        } else if (hasParent()) {
            return parent.getSchemaForDefinitionLevel(definitionLevel);
        }
        return null;
    }

    public boolean isRequired() {
        return modifier == Modifier.REQUIRED;
    }

    public void setRequired() {
        modifier = Modifier.REQUIRED;
    }

    public boolean isOptional() {
        return modifier == Modifier.OPTIONAL;
    }

    public void setOptional() {
        modifier = Modifier.OPTIONAL;
    }

    public boolean isRepeated() {
        return modifier == Modifier.REPEATED;
    }

    public void setRepeated() {
        modifier = Modifier.REPEATED;
    }

    /**
     * Returns the modified if the field represented by this schema, one of
     * REQUIRED, OPTIONAL or REPEATED.
     */
    public Modifier getModifier() {
        return modifier;
    }

    /**
     * Returns the list of child SchemaNodes of this schema. Must only be called
     * on a SchemaNode that represents a record field.
     */
    public List<SchemaNode> getFieldList() {
        if (type != Type.RECORD) {
            throw new RuntimeException("SchemaTree " + name
                    + " is not a record type.");
        }
        return fieldList;
    }

    /**
     * Adds the given SchemaNode to the list of children of this schema. Must
     * only be called on a SchemaNode that represents a record field.
     */
    public void addField(SchemaNode newField) {
        String name = newField.getName();
        if (type != Type.RECORD) {
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

    /**
     * Returns true when this SchemaNode has a child schema with the given name.
     */
    public boolean hasField(String fieldName) {
        return fieldMap.containsKey(fieldName);
    }

    /**
     * Returns the child SchemaNode with the given name.
     */
    public SchemaNode getField(String fieldName) {
        return fieldMap.get(fieldName);
    }

    /**
     * Returns a field in the schema for a fully qualified name by recursing the
     * schema tree.
     */
    public SchemaNode getFullyQualifiedField(String fieldName) {
        if (fieldName.startsWith(name)) {
            // remove an eventual prefix
            int firstDotIndex = fieldName.indexOf('.');
            fieldName = fieldName.substring(firstDotIndex + 1);
        }
        int firstDotIndex = fieldName.indexOf('.');
        if (firstDotIndex < 0) {
            if (fieldMap.containsKey(fieldName)) {
                return fieldMap.get(fieldName);
            } else {
                throw new RuntimeException("Field " + fieldName
                        + " not contained in " + getName() + ".");
            }
        }
        String firstPart = fieldName.substring(0, firstDotIndex);
        String secondPart = fieldName.substring(firstDotIndex + 1,
                fieldName.length());
        if (fieldMap.containsKey(firstPart)) {
            return fieldMap.get(firstPart).getFullyQualifiedField(secondPart);
        } else {
            throw new RuntimeException("Field " + firstPart
                    + " not contained in " + getName() + ".");
        }
    }

    /**
     * Returns true when this SchemaNode has a parent.
     */
    public boolean hasParent() {
        return parent != null;
    }

    /**
     * Returns the parent SchemaNode of this schema.
     */
    public SchemaNode getParent() {
        return parent;
    }

    /**
     * Returns a "projection" of this schema. That is a new tree of SchemaNodeS
     * that only contains those fields that are specified in projectedFields.
     * Also renames fields if they occur in the rename map.
     * 
     * <p>
     * The projection is performed recursively to process all SchemaNodes in the
     * tree.
     */
    public SchemaNode projectAndRename(Set<SchemaNode> projectedFields,
            Map<SchemaNode, String> renameMap) {
        return internalProject(this.parent, projectedFields, renameMap);
    }

    /**
     * Used internally by {@code project} to correctly set the parent of newly
     * created SchemaNodesS.
     */
    private SchemaNode internalProject(SchemaNode parent,
            Set<SchemaNode> projectedFields, Map<SchemaNode, String> renameMap) {
        assert (!isPrimitive());

        SchemaNode result = createRecord(this.getName());
        result.modifier = this.modifier;
        result.parent = parent;

        for (SchemaNode field : fieldList) {
            // first project out primitive fields
            if (field.isPrimitive() && projectedFields.contains(field)) {
                SchemaNode childField;
                if (renameMap.containsKey(field)) {
                    childField = createPrimitive(renameMap.get(field),
                            field.getPrimitiveType());
                } else {
                    childField = createPrimitive(field.getName(),
                            field.getPrimitiveType());
                }
                childField.modifier = field.modifier;
                result.addField(childField);
            } else if (!field.isPrimitive()) {
                SchemaNode projectedField = field.internalProject(result,
                        projectedFields, renameMap);
                if (projectedField.fieldList.size() > 0
                        || projectedFields.contains(projectedField)) {
                    result.addField(projectedField);
                }
            }
        }

        return result;
    }

    /**
     * Checks whether this SchemaNode represents the same schema as the given
     * object. Returns true if the schemas are equal.
     */
    public boolean equals(Object other) {
        if (!(other instanceof SchemaNode)) {
            return false;
        }

        SchemaNode otherSchema = (SchemaNode) other;

        if (getType() != otherSchema.getType()) {
            return false;
        }

        if (getModifier() != otherSchema.getModifier()) {
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

        if (!getName().equals(otherSchema.getName())) {
            return false;
        }
        return true;
    }

    /**
     * Returns the hash code of this schema. This is required to store
     * SchemaNodeS in hash maps, for example in the fieldMap of a SchemaNode.
     */
    public int hashCode() {
        return Objects.hashCode(fieldMap, primitiveType, getQualifiedName(),
                type, modifier);
    }

    /**
     * Returns a textual representation of the schema that is represented by the
     * tree of SchemaNodesS. This textual representation is compatible with
     * protobuf schema definitions.
     */
    public String toString() {
        return "package " + getName() + ";\n" + internalToStringRecursive(0);
    }

    /**
     * Used internally by {@code toString}.
     */
    private String internalToStringRecursive(int indentation) {
        StringBuffer identBuffer = new StringBuffer();
        for (int i = 0; i < indentation; i++) {
            identBuffer.append("      ");
        }
        indentation++;
        String indent = identBuffer.toString();

        String result = "";
        switch (type) {
        case PRIMITIVE:
            switch (modifier) {
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
            result += primitiveType.toString() + " " + name;
            break;
        case RECORD:
            result += "message " + name + " {\n";
            List<String> childStrings = Lists.newLinkedList();
            int count = 1;
            for (SchemaNode schema : fieldList) {
                childStrings.add(fieldMap.get(schema.getName())
                        .internalToStringRecursive(indentation)
                        + " = "
                        + count
                        + ";");
                ++count;
            }
            Joiner join = Joiner.on('\n');
            result += join.join(childStrings);
            result += " }";
            if (parent != null) {
                result += "\n" + indent;
                switch (modifier) {
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
                result += getName() + " " + getName();
            }
            break;
        default:
            throw new RuntimeException("Unexpected node type " + type);
        }

        return indent + result;
    }
}

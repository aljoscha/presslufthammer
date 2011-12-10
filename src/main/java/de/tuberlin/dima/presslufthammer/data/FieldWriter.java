package de.tuberlin.dima.presslufthammer.data;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public abstract class FieldWriter {
    private final FieldWriter parent;
    private final SchemaNode schema;
    private final Map<SchemaNode, FieldWriter> children;
    private final int treeDepth;
    private final String qualifiedName;

    protected FieldWriter(FieldWriter parent, SchemaNode schema) {
        this.parent = parent;
        this.schema = schema;

        if (this.parent == null) {
            treeDepth = 0;
            this.qualifiedName = schema.getName();
        } else {
            treeDepth = parent.getTreeDepth() + 1;
            this.qualifiedName = parent.getQualifiedName() + "."
                    + schema.getName();
        }

        this.children = Maps.newHashMap();
    }

    protected abstract void writeFieldInternal(Field field,
            int repetitionLevel, int definitionLevel);

    public final void writeField(Field field, int repetitionLevel) {
        writeFieldInternal(field, repetitionLevel, -1);
    }

    public final void addChild(SchemaNode schema, FieldWriter writer) {
        children.put(schema, writer);
    }

    public final boolean hasChild(SchemaNode schema) {
        return children.containsKey(schema);
    }

    public final FieldWriter getChild(SchemaNode schema) {
        return children.get(schema);
    }

    public final List<FieldWriter> getChildren() {
        return Lists.newLinkedList(children.values());
    }

    public final FieldWriter getParent() {
        return parent;
    }

    public final SchemaNode getSchema() {
        return schema;
    }

    public final int getTreeDepth() {
        return treeDepth;
    }

    public final String getQualifiedName() {
        return qualifiedName;
    }
}

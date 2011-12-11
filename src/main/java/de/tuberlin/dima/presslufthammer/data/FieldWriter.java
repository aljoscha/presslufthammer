package de.tuberlin.dima.presslufthammer.data;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public abstract class FieldWriter {
    private final FieldWriter parent;
    private final SchemaNode schema;
    private final Map<SchemaNode, FieldWriter> children;
    private final String qualifiedName;
    private final int repetition;
    private final int maxDefinition;

    List<LevelState> levelStates;
    int lastParentStateVersion;
    int parentStateInsertPoint;

    protected FieldWriter(FieldWriter parent, SchemaNode schema) {
        this.parent = parent;
        this.schema = schema;

        int parentMaxDefinition = 0;
        int parentRepetition = 0;
        if (this.parent == null) {
            this.qualifiedName = schema.getName();
        } else {
            this.qualifiedName = parent.getQualifiedName() + "."
                    + schema.getName();
            parentRepetition = parent.getRepetition();
            parentMaxDefinition = parent.getMaxDefinition();
        }

        if (schema.isOptional() || schema.isRepeated()) {
            maxDefinition = parentMaxDefinition + 1;
        } else {
            maxDefinition = parentMaxDefinition;
        }
        if (schema.isRepeated()) {
            repetition = parentRepetition + 1;
        } else {
            repetition = parentRepetition;
        }

        this.children = Maps.newHashMap();
        this.levelStates = Lists.newLinkedList();
        lastParentStateVersion = -1;
        parentStateInsertPoint = 0;
    }

    protected abstract void writeFieldInternal(Field field,
            int repetitionLevel, int definitionLevel);

    public final void writeField(Field field, int repetitionLevel) {
        if (field != null) {
            if (parent != null) {
                int parentState = parent.getStateVersion();
                if (parentState > lastParentStateVersion) {
                    List<LevelState> parentStates = parent
                            .getLevelStates(lastParentStateVersion);
                    lastParentStateVersion = parentState;
                    for (int i = 0; i < parentStates.size(); ++i) {
                        LevelState currentState = parentStates.get(i);
                        LevelState nextState = null;
                        if (i + 1 < parentStates.size()) {
                            nextState = parentStates.get(i + 1);
                        } else {
                            nextState = new LevelState(repetitionLevel,
                                    maxDefinition);
                            if (schema.isRequired()) {
                                nextState.d += 1;
                            }
                        }

                        if (nextState.r == currentState.r
                                && nextState.d > currentState.d) {
                            continue;
                        }
                        writeFieldInternal(null, currentState.r, currentState.d);
                    }
                }
            }
            writeFieldInternal(field, repetitionLevel, maxDefinition);
        } else {
            fetchParentStates();
        }
        levelStates.add(new LevelState(repetitionLevel, maxDefinition));
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

    public final int getRepetition() {
        return repetition;
    }

    public final String getQualifiedName() {
        return qualifiedName;
    }

    public final int getMaxDefinition() {
        return maxDefinition;
    }

    private void fetchParentStates() {
        if (parent != null) {
            int parentStateVersion = parent.getStateVersion();
            if (parentStateVersion > lastParentStateVersion) {
                List<LevelState> parentStates = parent
                        .getLevelStates(lastParentStateVersion);
                for (LevelState parentState : parentStates) {
                    levelStates.add(parentState);
                }
                lastParentStateVersion = parentStateVersion;
            }
        }
    }

    private List<LevelState> getLevelStates(int version) {
        fetchParentStates();
        if (version + 1 > levelStates.size()) {
            return Lists.newLinkedList();
        }
        return levelStates.subList(version + 1, levelStates.size());
    }

    private int getStateVersion() {
        fetchParentStates();
        return levelStates.size() - 1;
    }

    public void finalizeLevels() {
        if (schema.isPrimitive()) {
            if (parent != null) {
                int parentState = parent.getStateVersion();
                if (parentState > lastParentStateVersion) {
                    List<LevelState> parentStates = parent
                            .getLevelStates(lastParentStateVersion);
                    lastParentStateVersion = parentState;
                    for (int i = 0; i < parentStates.size(); ++i) {
                        LevelState currentState = parentStates.get(i);
                        LevelState nextState = null;
                        if (i + 1 < parentStates.size()) {
                            nextState = parentStates.get(i + 1);
                        } else {
                            nextState = new LevelState(repetition,
                                    maxDefinition);
                            if (schema.isRequired()) {
                                nextState.d += 1;
                            }
                        }

                        if (nextState.r == currentState.r
                                && nextState.d > currentState.d) {
                            continue;
                        }
                        writeFieldInternal(null, currentState.r, currentState.d);
                    }
                }
            }
        } else if (schema.isRecord()) {
            for (FieldWriter child : children.values()) {
                child.finalizeLevels();
            }
        }
    }

    protected class LevelState {
        public int r;
        public int d;

        public LevelState(int r, int d) {
            this.r = r;
            this.d = d;
        }
    }
}

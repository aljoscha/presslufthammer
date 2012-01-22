package de.tuberlin.dima.presslufthammer.data;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.hierarchical.Field;

/**
 * This is used to construct the tree of field writers in the column striping
 * algorithm ({@link FieldStriper}).
 * 
 * <p>
 * Objects of this class store the parent field writer, the schema of the field
 * represented by this writer the actual {@link ColumnWriter} for writing out
 * the columnar data of this field and a list of child writers (implemented as a
 * map from SchemaNode).
 * 
 * @author Aljoscha Krettek
 * 
 */
public final class FieldWriter {
    private final FieldWriter parent;
    private final SchemaNode schema;
    private final ColumnWriter writer;
    private final Map<SchemaNode, FieldWriter> children;
    private final int repetition;
    private final int maxDefinition;

    List<LevelState> levelStates;
    int lastParentStateVersion;
    int parentStateInsertPoint;

    /**
     * Constructs aa field writer for the given schema and sets its parent and
     * column writer accordingly. Intitially a field writer has no children and
     * it contains no level states and has not synced to states from a parent
     * (see dremel paper for explanation).
     */
    public FieldWriter(FieldWriter parent, SchemaNode schema,
            ColumnWriter writer) {
        this.parent = parent;
        this.schema = schema;
        this.writer = writer;

        maxDefinition = schema.getMaxDefinition();
        repetition = schema.getRepetition();

        this.children = Maps.newHashMap();
        this.levelStates = Lists.newLinkedList();
        lastParentStateVersion = -1;
        parentStateInsertPoint = 0;
    }

    /**
     * Writes one field value using this writer. This method takes care of
     * syncing the state of this writer to the parent states and writing
     * eventual repetition/definition levels before writing the field and
     * repetition level that are given as argument. One can also pass null as
     * the field argument, in that case this field writer will only sync to the
     * parent states.
     * 
     */
    public final void writeField(Field field, int repetitionLevel)
            throws IOException {
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
                        writer.writeNull(currentState.r, currentState.d);
                    }
                }
            }
            writer.writeField(field, repetitionLevel, maxDefinition);
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

    public final int getMaxDefinition() {
        return maxDefinition;
    }

    /**
     * This method and the next two are responsible for the syncing of states
     * from parents to child field writers. This is still a bit of a hack and
     * does not work the same as in the paper (it works correctly though, only
     * possibly not as efficient), should probably fix this at some point.
     */
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

    /**
     * {@see fetchParentStates}
     */
    private List<LevelState> getLevelStates(int version) {
        fetchParentStates();
        if (version + 1 > levelStates.size()) {
            return Lists.newLinkedList();
        }
        return levelStates.subList(version + 1, levelStates.size());
    }

    /**
     * {@see fetchParentStates}
     */
    private int getStateVersion() {
        fetchParentStates();
        return levelStates.size() - 1;
    }

    /**
     * This method is called at the end of the striping process to flush
     * eventual parent states that have not yet been synced by child field
     * writers. If the schema of this field writer is a primitive field the
     * state is synced, otherwise the method is called recursively on all child
     * fields.
     * 
     */
    public void finalizeLevels() throws IOException {
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
                        }

                        if (nextState != null && nextState.r == currentState.r
                                && nextState.d > currentState.d) {
                            continue;
                        }
                        writer.writeNull(currentState.r, currentState.d);
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

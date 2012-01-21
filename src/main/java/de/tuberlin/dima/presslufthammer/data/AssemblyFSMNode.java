package de.tuberlin.dima.presslufthammer.data;

import java.util.Map;

import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;

/**
 * This represents one node in the FSM created by {@link AssemblyFSM}.
 * 
 * <p>
 * It basically holds the corresponding {@link SchemaNode}, the
 * {@link ColumnReader} from the {@link Tablet} that is the source and the
 * transitions to the other nodes in the FSM.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class AssemblyFSMNode {
    private SchemaNode schema;
    private ColumnReader reader;
    private Map<Integer, AssemblyFSMNode> transitions;

    /**
     * Constructs the node from the given schema, at the beginning it has no
     * transitions.
     */
    public AssemblyFSMNode(SchemaNode schema) {
        this.schema = schema;
        transitions = Maps.newHashMap();
    }

    /**
     * Used to represent the final state of the FSM.
     */
    private AssemblyFSMNode() {
        this(null);
    }

    /**
     * Constructs a node that is a final state.
     */
    public static AssemblyFSMNode getFinalState() {
        return new AssemblyFSMNode();
    }

    /**
     * Returns true when this node is a final node.
     */
    public boolean isFinal() {
        return schema == null;
    }

    /**
     * Returns true when this node has a transition to another
     * {@link AssemblyFSMNode} for the given repetition level.
     */
    public boolean hasTransition(int level) {
        return transitions.containsKey(level);
    }

    /**
     * Adds a transition to another {@link AssemblyFSMNode} for the given
     * repetition level.
     */
    public void setTransition(int level, AssemblyFSMNode node) {
        transitions.put(level, node);
    }

    /**
     * Returns the {@link AssemblyFSMNode} that the fsm transitions to given the
     * repetition level. This should only be called when there actually is a
     * transition for the level.
     */
    public AssemblyFSMNode getTransition(int level) {
        if (!transitions.containsKey(level)) {
            throw new RuntimeException(
                    "This should not happen, bug in code. Schema: "
                            + schema.getQualifiedName() + " level: " + level);
        }
        return transitions.get(level);
    }

    /**
     * Sets the column reader for this node, used in the {@link AssemblyFSM}
     * during initialization.
     */
    public void setReader(ColumnReader reader) {
        this.reader = reader;
    }

    /**
     * Returns the column reader that is attached to this node.
     */
    public ColumnReader getReader() {
        return reader;
    }

    /**
     * Reuturns the {@link SchemaNode} that is attached to this node.
     */
    public SchemaNode getSchema() {
        return schema;
    }

    /**
     * For debugging...
     */
    @Override
    public String toString() {
        if (isFinal()) {
            return "FINAL FSM NODE\n";
        }

        StringBuilder result = new StringBuilder();
        result.append("FSM Node: " + schema.getQualifiedName());
        result.append("\n");
        for (Integer level : transitions.keySet()) {
            if (transitions.get(level).isFinal()) {

                result.append(level + " -> FINAL\n");
            } else {
                result.append(level + " -> "
                        + transitions.get(level).getSchema().getQualifiedName()
                        + "\n");
            }
        }
        return result.toString();
    }
}

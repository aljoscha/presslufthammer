package de.tuberlin.dima.presslufthammer.data;

import java.util.List;

import com.google.common.collect.Lists;

public class AssemblyFSM {
    private List<AssemblyFSMNode> nodes;

    public AssemblyFSM(SchemaNode schema) {
        nodes = Lists.newLinkedList();

        constructFSM(schema);
    }

    public AssemblyFSMNode getStartNode() {
        return nodes.get(0);
    }

    private void constructFSM(SchemaNode schema) {
        List<SchemaNode> fields = constructFieldList(schema);
        for (int i = 0; i < fields.size(); ++i) {
            SchemaNode field = fields.get(i);
            nodes.add(new AssemblyFSMNode(field));
        }
        nodes.add(AssemblyFSMNode.getFinalState());

        for (int i = 0; i < fields.size(); ++i) {
            SchemaNode field = fields.get(i);
            AssemblyFSMNode fsmNode = nodes.get(i);
            int maxLevel = field.getRepetition();
            AssemblyFSMNode barrier = nodes.get(i + 1);
            int barrierLevel;
            if (barrier.isFinal()) {
                barrierLevel = 0;
            } else {
                barrierLevel = commonRepetitionLevel(field, barrier.getSchema());
            }

            for (int j = i; j >= 0; --j) {
                SchemaNode preField = fields.get(j);
                if (preField.getRepetition() <= barrierLevel) {
                    continue;
                }
                int backLevel = commonRepetitionLevel(field, preField);
                fsmNode.setTransition(backLevel, nodes.get(j));
            }

            for (int level = barrierLevel + 1; level <= maxLevel; ++level) {
                if (!fsmNode.hasTransition(level)) {
                    if (fsmNode.hasTransition(level - 1)) {
                        fsmNode.setTransition(level,
                                fsmNode.getTransition(level - 1));
                    }
                }
            }

            for (int level = 0; level <= barrierLevel; ++level) {
                fsmNode.setTransition(level, barrier);
            }
        }
    }

    private int commonRepetitionLevel(SchemaNode field1, SchemaNode field2) {
        if (field1.equals(field2)) {
            return field1.getRepetition();
        }
        SchemaNode parent1 = field1.getParent();
        SchemaNode parent2 = field2.getParent();

        while (parent1 != null && parent2 != null && !parent1.equals(parent2)) {
            if (parent1.getRepetition() >= parent2.getRepetition()) {
                parent1 = parent1.getParent();
            } else {
                parent2 = parent2.getParent();
            }
        }

        if (parent1 != null && parent2 != null && parent1.equals(parent2)) {
            return parent1.getRepetition();
        } else {
            return 0;
        }
    }

    private List<SchemaNode> constructFieldList(SchemaNode schema) {
        if (schema.isRecord()) {
            List<SchemaNode> result = Lists.newLinkedList();
            for (SchemaNode childSchema : schema.getFieldList()) {
                result.addAll(constructFieldList(childSchema));
            }
            return result;
        } else {
            List<SchemaNode> result = Lists.newArrayList(schema);
            return result;
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (AssemblyFSMNode node : nodes) {
            result.append(node.toString());
        }
        return result.toString();
    }
}

package de.tuberlin.dima.presslufthammer.data;

import java.util.List;

import com.google.common.collect.Lists;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordEncoder;
import de.tuberlin.dima.presslufthammer.data.hierarchical.RecordStore;

public class AssemblyFSM {
    private List<AssemblyFSMNode> nodes;
    private SchemaNode schema;

    public AssemblyFSM(SchemaNode schema) {
        nodes = Lists.newLinkedList();
        this.schema = schema;

        constructFSM(schema);
    }

    public void assembleRecords(Tablet source, RecordStore target) {
        initializeReaders(source);

        AssemblyFSMNode startNode = nodes.get(0);
        ColumnReader reader = startNode.getReader();
        while (reader.hasNext()) {
            assembleRecord(target);
        }
    }

    private void adjustRecordStructure(ColumnReader reader,
            RecordEncoder record, SchemaNode currentSchema,
            SchemaNode lastSchema) {
        // "moveTo"
        SchemaNode commonAncestor = commonAncestor(currentSchema, lastSchema);
        if (commonAncestor.equals(currentSchema)) {
            // No need to to anything
        }

        record.returnToLevel(commonAncestor);
        if (currentSchema.isFirstField() && commonAncestor.hasParent()) {
            record.returnToLevel(commonAncestor.getParent());
        }

        if (reader.getNextDefinition() >= currentSchema.getMaxDefinition()) {
            record.moveToLevel(currentSchema.getParent());
        } else {
            // just move to the correct level for the definition level of the
            // reader
            SchemaNode correctSchema = currentSchema
                    .getSchemaForDefinitionLevel(reader.getNextDefinition());
            record.moveToLevel(correctSchema);
        }
    }

    private void assembleRecord(RecordStore target) {
        RecordEncoder record = target.startRecord();
        AssemblyFSMNode lastNode = null;
        SchemaNode lastSchema = schema;
        AssemblyFSMNode currentNode = nodes.get(0);
        SchemaNode currentSchema = currentNode.getSchema();
        ColumnReader reader = currentNode.getReader();

        while (reader != null && reader.hasNext()) {
//            System.out.println("READER: " + currentSchema.getQualifiedName());
            adjustRecordStructure(reader, record, currentSchema, lastSchema);
            Object value = reader.getNextValue();
            if (value != null) {
                record.appendValue(currentSchema, value);
            }
            lastNode = currentNode;
            lastSchema = lastNode.getSchema();
//            System.out.println("NEXT REP: " + reader.getNextRepetition());
            currentNode = currentNode.getTransition(reader.getNextRepetition());
//            System.out.println(currentNode.toString());
            reader = currentNode.getReader();
            currentSchema = currentNode.getSchema();
            if (currentSchema != null) {
                record.returnToLevel(currentSchema.getParent());
            }
        }
//        if (reader == null) {
//            System.out.println("READER IS NULL");
//        }
        record.returnToLevel(schema);
        record.finalizeRecord();
    }

    private void initializeReaders(Tablet tablet) {
        for (AssemblyFSMNode node : nodes) {
            if (!node.isFinal()) {
                node.setReader(tablet.getColumnReader(node.getSchema()));
            }
        }
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

    private SchemaNode commonAncestor(SchemaNode field1, SchemaNode field2) {
        SchemaNode parent1 = field1;
        SchemaNode parent2 = field2;

        while (parent1 != null && parent2 != null && !parent1.equals(parent2)) {
            if (parent1.getFullDefinition() >= parent2.getFullDefinition()) {
                parent1 = parent1.getParent();
            } else {
                parent2 = parent2.getParent();
            }
        }

        if (parent1 != null && parent2 != null && parent1.equals(parent2)) {
            return parent1;
        } else {
            return null;
        }
    }

    private int commonRepetitionLevel(SchemaNode field1, SchemaNode field2) {
        SchemaNode commonAncestor = commonAncestor(field1, field2);
        if (commonAncestor == null) {
            return 0;
        } else {
            return commonAncestor.getRepetition();
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

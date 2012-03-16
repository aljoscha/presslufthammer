package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.qexec.grouping.GroupField;
import de.tuberlin.dima.presslufthammer.query.SelectClause;
import de.tuberlin.dima.presslufthammer.query.SelectClause.Aggregation;

/**
 * The emitter is responsible for actually emitting values from a Slice to a
 * target tablet. Grouping and aggregation is also performed here. We are using
 * hash bases aggregation, so when there are to many groups the query will fail.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class Emitter {
    private Tablet targetTablet;
    private QueryHelper helper;

    // no need for the group by fields, since we assume that
    // the query is semantically correct at this point we can assume that
    // fields that are in simple fields must be grouping fields
    // when there are fields in the aggregation fields
    private List<SchemaNode> simpleSelectedFields;
    private List<SchemaNode> aggregatedSelectedFields;

    private boolean hasAggregation = false;
    private Map<Long, Map<SchemaNode, GroupField>> aggregationMap;

    private Map<SchemaNode, ColumnWriter> fieldWriters;

    private int selectLevel = 0;

    public Emitter(Tablet targetTablet, QueryHelper helper) {
        this.helper = helper;
        this.targetTablet = targetTablet;

        simpleSelectedFields = Lists.newLinkedList();
        aggregatedSelectedFields = Lists.newLinkedList();

        fieldWriters = Maps.newHashMap();

        for (SelectClause clause : helper.getOriginalQuery().getSelectClauses()) {
            if (clause.getColumn().equals("*")) {
                continue;
            }
            if (clause.getAggregation() == Aggregation.NONE) {
                SchemaNode originalField = helper.getOriginalSchema()
                        .getFullyQualifiedField(clause.getColumn());
                SchemaNode renamedField = originalField;
                if (clause.getRenameAs() != null) {
                    renamedField = helper.getResultSchema()
                            .getFullyQualifiedField(clause.getRenamedColumn());
                }
                simpleSelectedFields.add(originalField);
                fieldWriters.put(originalField,
                        targetTablet.getColumnWriter(renamedField));
            } else {
                SchemaNode originalField = helper.getOriginalSchema()
                        .getFullyQualifiedField(clause.getColumn());
                aggregatedSelectedFields.add(originalField);
                hasAggregation = true;
            }
        }

        if (hasAggregation) {
            aggregationMap = Maps.newHashMap();
        }
    }

    public int getSelectLevel() {
        return selectLevel;
    }

    public void setSelectLevel(int selectLevel) {
        this.selectLevel = selectLevel;
    }

    public void finalizeGroups() throws IOException {
        if (hasAggregation) {
            for (Map<SchemaNode, GroupField> group : aggregationMap.values()) {
                for (GroupField field : group.values()) {
                    field.emit();
                }
            }
        }
    }

    public void emit(Slice slice) throws IOException {

        if (hasAggregation) {
            long groupKey = determineGroupKey(slice);
            if (!aggregationMap.containsKey(groupKey)) {
                aggregationMap.put(groupKey,
                        helper.createAggregationGroup(targetTablet));
            }

            Map<SchemaNode, GroupField> group = aggregationMap.get(groupKey);

            for (SchemaNode field : simpleSelectedFields) {
                ColumnReader column = slice.getColumn(field);
                if (column.getCurrentRepetition() >= selectLevel) {
                    group.get(field).addValue(column);
                    column.advanceWriteRepetition();
                }
            }

            for (SchemaNode field : aggregatedSelectedFields) {
                ColumnReader column = slice.getColumn(field);
                if (column.getCurrentRepetition() >= selectLevel) {
                    group.get(field).addValue(column);
                    column.advanceWriteRepetition();
                }
            }

        } else {
            for (SchemaNode field : simpleSelectedFields) {
                ColumnReader column = slice.getColumn(field);
                if (column.getCurrentRepetition() >= selectLevel) {
                    column.writeToColumn(fieldWriters.get(field));
                }
            }
        }
    }

    private long determineGroupKey(Slice slice) {
        Object[] groupFields = new Object[simpleSelectedFields.size()];
        int count = 0;
        for (SchemaNode groupByField : simpleSelectedFields) {
            groupFields[count++] = slice.getColumn(groupByField).getValue();
        }
        return Objects.hashCode(groupFields);
    }

}

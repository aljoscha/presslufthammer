package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryWriteonlyTablet;
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
    private InMemoryWriteonlyTablet targetTablet;

    private SchemaNode resultSchema;
    private Map<SchemaNode, SchemaNode> renameMap;
    // no need for the group by fields, since we assume that
    // the query is semantically correct at this point we can assume that
    // fields that are in simple fields must be grouping fields
    // when there are fields in the aggregation fields
    private List<SchemaNode> simpleSelectedFields;
    private List<SchemaNode> aggregatedSelectedFields;

    private Map<SchemaNode, FieldEmitter> fieldEmitters;

    private int selectLevel = 0;

    public Emitter(InMemoryWriteonlyTablet targetTablet, QueryHelper helper) {
        this.targetTablet = targetTablet;
        this.renameMap = helper.getRenameMap();

        simpleSelectedFields = Lists.newLinkedList();
        aggregatedSelectedFields = Lists.newLinkedList();

        fieldEmitters = Maps.newHashMap();

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
                fieldEmitters.put(
                        originalField,
                        new FieldEmitter(targetTablet
                                .getColumnWriter(renamedField)));
            }
        }
    }

    public int getSelectLevel() {
        return selectLevel;
    }

    public void setSelectLevel(int selectLevel) {
        this.selectLevel = selectLevel;
    }

    public void emit(Slice slice) throws IOException {

        for (SchemaNode field : simpleSelectedFields) {
            ColumnReader column = slice.getColumn(field);
            if (column.getCurrentRepetition() >= selectLevel) {
                fieldEmitters.get(field).emit(column);
            }
        }
    }

    public InMemoryWriteonlyTablet getResultTablet() {
        return targetTablet;
    }
}

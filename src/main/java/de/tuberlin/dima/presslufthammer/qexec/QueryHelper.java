package de.tuberlin.dima.presslufthammer.qexec;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import de.tuberlin.dima.presslufthammer.data.PrimitiveType;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.qexec.grouping.GroupField;
import de.tuberlin.dima.presslufthammer.qexec.grouping.Int64CountGroupField;
import de.tuberlin.dima.presslufthammer.qexec.grouping.Int64PlainGroupField;
import de.tuberlin.dima.presslufthammer.qexec.grouping.Int64SumGroupField;
import de.tuberlin.dima.presslufthammer.qexec.grouping.StringCountGroupField;
import de.tuberlin.dima.presslufthammer.qexec.grouping.StringPlainGroupField;
import de.tuberlin.dima.presslufthammer.qexec.grouping.StringSumGroupField;
import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.query.SelectClause;
import de.tuberlin.dima.presslufthammer.query.SelectClause.Aggregation;
import de.tuberlin.dima.presslufthammer.query.WhereClause;

/**
 * For support during query execution, this class analyzes a query, rewrites the
 * query for child queries (i.e. aggregations), determines the result schema and
 * determines the involved table fields (columns).
 * 
 * The "rewritten" queries need not necessarily be rewritten (they are only
 * rewritten in case of aggregation queries).
 * 
 * @author Aljoscha Krettek
 * 
 */
public class QueryHelper {
    private final Logger log = LoggerFactory.getLogger(QueryHelper.class);
    private SchemaNode originalSchema;
    private SchemaNode resultSchema;
    private Set<SchemaNode> involvedFields;
    private Set<SchemaNode> selectedFields;
    private Set<SchemaNode> whereClauseFields;
    private Map<SchemaNode, SchemaNode> renameMap;
    private Query originalQuery;
    private Query rewrittenQuery;
    private Query rewrittenChildQuery;

    public QueryHelper(Query query, SchemaNode schema) {
        this.originalSchema = schema;
        analyzeQuery(query);
    }

    private void analyzeQuery(Query query) {
        this.originalQuery = query;
        involvedFields = Sets.newHashSet();
        selectedFields = Sets.newHashSet();
        whereClauseFields = Sets.newHashSet();
        Map<SchemaNode, String> internalRenameMap = Maps.newHashMap();

        // check for SELECT * QUERY
        if (query.getSelectClauses().size() == 1
                && query.getSelectClauses().get(0).getColumn().equals("*")) {
            query.getSelectClauses().remove(0);
            List<SchemaNode> allFields = getAllSchemaFields(originalSchema);
            for (SchemaNode field : allFields) {
                query.getSelectClauses().add(
                        new SelectClause(field.getQualifiedName(),
                                Aggregation.NONE, null));
            }
        }

        // first for all select clauses
        for (SelectClause selectClause : query.getSelectClauses()) {
            SchemaNode field = originalSchema
                    .getFullyQualifiedField(selectClause.getColumn());
            involvedFields.add(field);
            selectedFields.add(field);
            if (selectClause.getRenameAs() != null) {
                internalRenameMap.put(field, selectClause.getRenameAs());
            }
        }

        // then the where clauses
        for (WhereClause whereClause : query.getWhereClauses()) {
            involvedFields.add(originalSchema
                    .getFullyQualifiedField(whereClause.getColumn()));
            whereClauseFields.add(originalSchema
                    .getFullyQualifiedField(whereClause.getColumn()));
        }

        // We do not need to add group by fields because they are required
        // to be in select clauses anyway

        // project out the fields that are not selected for the result
        // schema, then eventually rename fields or change types (aggregation)
        resultSchema = originalSchema.projectAndRename(selectedFields,
                internalRenameMap);

        renameMap = Maps.newHashMap();
        for (SelectClause selectClause : query.getSelectClauses()) {
            if (selectClause.getRenameAs() != null) {
                renameMap.put(originalSchema
                        .getFullyQualifiedField(selectClause.getColumn()),
                        resultSchema.getFullyQualifiedField(selectClause
                                .getRenamedColumn()));
            }
        }

        // change field types in new schema for aggregation fields
        for (SelectClause selectClause : query.getSelectClauses()) {
            if (selectClause.getAggregation() == Aggregation.COUNT) {
                SchemaNode field = resultSchema
                        .getFullyQualifiedField(selectClause.getRenamedColumn());
                field.setPrimitiveType(PrimitiveType.INT64);
                field.setOptional();
            } else if (selectClause.getAggregation() == Aggregation.SUM) {
                SchemaNode field = resultSchema
                        .getFullyQualifiedField(selectClause.getRenamedColumn());
                field.setOptional();
            }
        }
        log.debug("Original schema:\n{}", originalSchema.toString());
        log.debug("Result schema:\n{}", resultSchema.toString());

        // and now finally rewrite the queries (if necessary)
        // remove and renames from query (those are pushed down to child)
        List<SelectClause> rewrittenSelectClauses = Lists.newLinkedList();
        for (SelectClause selectClause : query.getSelectClauses()) {
            SelectClause rewrittenSelectClause;
            if (selectClause.getAggregation() == Aggregation.COUNT) {
                rewrittenSelectClause = new SelectClause(
                        selectClause.getRenamedColumn(), Aggregation.SUM, null);
            } else {
                rewrittenSelectClause = new SelectClause(
                        selectClause.getRenamedColumn(),
                        selectClause.getAggregation(), null);
            }
            rewrittenSelectClauses.add(rewrittenSelectClause);
        }
        // rewritten query has no where clauses, only child queries have
        // where clauses
        rewrittenQuery = new Query(query.getTableName(), query.getPartition(),
                rewrittenSelectClauses, new LinkedList<WhereClause>(),
                query.getGroupByColumns());

        rewrittenSelectClauses = Lists.newLinkedList();
        for (SelectClause selectClause : query.getSelectClauses()) {
            SelectClause rewrittenSelectClause = new SelectClause(
                    selectClause.getColumn(), selectClause.getAggregation(),
                    selectClause.getRenameAs());
            rewrittenSelectClauses.add(rewrittenSelectClause);
        }
        rewrittenChildQuery = new Query(query.getTableName(),
                query.getPartition(), rewrittenSelectClauses,
                query.getWhereClauses(), query.getGroupByColumns());

        log.debug("Rewritten query: {}", rewrittenQuery);
        log.debug("Rewritten child query: {}", rewrittenChildQuery);
    }

    public Map<SchemaNode, GroupField> createAggregationGroup(
            Tablet targetTablet) {
        Map<SchemaNode, GroupField> fields = Maps.newHashMap();

        for (SelectClause clause : originalQuery.getSelectClauses()) {
            if (clause.getColumn().equals("*")) {
                continue;
            }
            if (clause.getAggregation() == Aggregation.NONE) {
                SchemaNode originalField = originalSchema
                        .getFullyQualifiedField(clause.getColumn());
                SchemaNode renamedField = originalField;
                if (clause.getRenameAs() != null) {
                    renamedField = resultSchema.getFullyQualifiedField(clause
                            .getRenamedColumn());
                }
                fields.put(
                        originalField,
                        createPlainGroupField(renamedField,
                                targetTablet.getColumnWriter(renamedField)));

            } else if (clause.getAggregation() == Aggregation.SUM) {
                SchemaNode originalField = originalSchema
                        .getFullyQualifiedField(clause.getColumn());
                SchemaNode renamedField = resultSchema
                        .getFullyQualifiedField(clause.getRenamedColumn());
                fields.put(
                        originalField,
                        createSumGroupField(renamedField,
                                targetTablet.getColumnWriter(renamedField)));

            } else if (clause.getAggregation() == Aggregation.COUNT) {
                SchemaNode originalField = originalSchema
                        .getFullyQualifiedField(clause.getColumn());
                SchemaNode renamedField = resultSchema
                        .getFullyQualifiedField(clause.getRenamedColumn());
                fields.put(
                        originalField,
                        createCountGroupField(renamedField,
                                targetTablet.getColumnWriter(renamedField)));
            }
        }
        return fields;
    }

    private GroupField createPlainGroupField(SchemaNode schema,
            ColumnWriter writer) {
        switch (schema.getPrimitiveType()) {
        case INT64:
            return new Int64PlainGroupField(writer);
        case STRING:
            return new StringPlainGroupField(writer);
        default:
            throw new RuntimeException("Unknown schema "
                    + schema.getPrimitiveType()
                    + " for grouping field generation.");
        }
    }

    private GroupField createSumGroupField(SchemaNode schema,
            ColumnWriter writer) {
        switch (schema.getPrimitiveType()) {
        case INT64:
            return new Int64SumGroupField(writer);
        case STRING:
            return new StringSumGroupField(writer);
        default:
            throw new RuntimeException("Unknown schema "
                    + schema.getPrimitiveType()
                    + " for grouping field generation.");
        }
    }

    private GroupField createCountGroupField(SchemaNode schema,
            ColumnWriter writer) {
        switch (schema.getPrimitiveType()) {
        case INT64:
            return new Int64CountGroupField(writer);
        case STRING:
            return new StringCountGroupField(writer);
        default:
            throw new RuntimeException("Unknown schema "
                    + schema.getPrimitiveType()
                    + " for grouping field generation.");
        }
    }

    private List<SchemaNode> getAllSchemaFields(SchemaNode schema) {
        List<SchemaNode> result = Lists.newLinkedList();
        getAllSchemaFieldsRecursive(schema, result);
        return result;
    }

    private void getAllSchemaFieldsRecursive(SchemaNode schema,
            List<SchemaNode> result) {
        if (schema.isRecord()) {
            for (SchemaNode child : schema.getFieldList()) {
                getAllSchemaFieldsRecursive(child, result);
            }
        } else {
            result.add(schema);
        }
    }

    public SchemaNode getOriginalSchema() {
        return originalSchema;
    }

    public SchemaNode getResultSchema() {
        return resultSchema;
    }

    public Map<SchemaNode, SchemaNode> getRenameMap() {
        return renameMap;
    }

    public Set<SchemaNode> getInvolvedFields() {
        return involvedFields;
    }

    public Set<SchemaNode> getWhereClauseFields() {
        return whereClauseFields;
    }

    public Query getOriginalQuery() {
        return originalQuery;
    }

    public Query getRewrittenQuery() {
        return rewrittenQuery;
    }

    public Query getRewrittenChildQuery() {
        return rewrittenChildQuery;
    }

    public Emitter createEmitter(Tablet tablet) {
        Emitter emitter = new Emitter(tablet, this);
        return emitter;
    }
}

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
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryWriteonlyTablet;
import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.query.SelectClause;
import de.tuberlin.dima.presslufthammer.query.SelectClause.Aggregation;
import de.tuberlin.dima.presslufthammer.query.WhereClause;
import de.tuberlin.dima.presslufthammer.util.Config.TableConfig;

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

    public QueryHelper(Query query, TableConfig table) {
        this.originalSchema = table.getSchema();
        analyzeQuery(query);
    }

    public QueryHelper(Query query, Tablet sourceTablet) {
        this.originalSchema = sourceTablet.getSchema();
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
            }
        }
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

    public Emitter createEmitter() {
        InMemoryWriteonlyTablet resultTablet = new InMemoryWriteonlyTablet(
                resultSchema);
        Emitter emitter = new Emitter(resultTablet, this);
        return emitter;
    }
}

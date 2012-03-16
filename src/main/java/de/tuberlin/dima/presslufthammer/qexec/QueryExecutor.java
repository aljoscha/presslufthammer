package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryWriteonlyTablet;
import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.query.SelectClause;
import de.tuberlin.dima.presslufthammer.query.WhereClause;
import de.tuberlin.dima.presslufthammer.query.WhereClause.Op;

/**
 * The executor is responsible for executing a query given a source tablet. The
 * result is again a tablet.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class QueryExecutor {
    //private final Logger log = LoggerFactory.getLogger(getClass());

    private Tablet sourceTablet;
    private Query query;

    public QueryExecutor(Tablet sourceTablet, Query query) {
        this.sourceTablet = sourceTablet;
        this.query = query;
    }

    public InMemoryWriteonlyTablet performQuery() throws IOException {
        Set<String> projectedFields = Sets.newHashSet();
        for (SelectClause selectClause : query.getSelectClauses()) {
            projectedFields.add(selectClause.getColumn());
        }

        QueryHelper helper = new QueryHelper(query, sourceTablet);

        List<SchemaNode> involvedFields = Lists.newArrayList(helper
                .getInvolvedFields());
        
        Slice slice = new Slice(sourceTablet, involvedFields);

        Emitter emitter = helper.createEmitter();

        performSelectProjectAggregate(slice, helper, emitter);

        return emitter.getResultTablet();
    }

    private void performSelectProjectAggregate(Slice slice, QueryHelper helper,
            Emitter emitter) throws IOException {
        List<SchemaNode> whereClauseFields = Lists.newLinkedList(helper
                .getWhereClauseFields());

        while (slice.hasNext()) {
            slice.fetch();

            if (evalWhereClause(slice, whereClauseFields)) {
                emitter.emit(slice);
                emitter.setSelectLevel(slice.getFetchLevel());
            } else {
                emitter.setSelectLevel(Math.min(slice.getFetchLevel(),
                        emitter.getSelectLevel()));
            }
        }
    }

    private boolean evalWhereClause(Slice slice, List<SchemaNode> selectedFields)
            throws IOException {
        if (query.getWhereClauses().size() <= 0) {
            return true;
        }
        for (SchemaNode schema : selectedFields) {
            if (slice.getColumn(schema).isNull()) {
                continue;
            }
            for (WhereClause clause : query.getWhereClauses()) {
                if (schema.getQualifiedName().equals(clause.getColumn())) {
                    if (clause.getOp() == Op.EQ
                            && slice.getColumn(schema).getValue().toString()
                                    .equals(clause.getValue())) {
                        return true;
                    } else if (clause.getOp() == Op.NEQ
                            && !slice.getColumn(schema).getValue().toString()
                                    .equals(clause.getValue())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}

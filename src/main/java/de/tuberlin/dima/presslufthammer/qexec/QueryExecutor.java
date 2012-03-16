package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryWriteonlyTablet;
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
    // private final Logger log = LoggerFactory.getLogger(getClass());

    private QueryHelper helper;
    private List<SchemaNode> involvedFields;
    private InMemoryWriteonlyTablet resultTablet;
    private Emitter emitter;

    public QueryExecutor(QueryHelper helper) {
        this.helper = helper;
        involvedFields = Lists.newArrayList(helper.getInvolvedFields());
        resultTablet = new InMemoryWriteonlyTablet(helper.getResultSchema());
        emitter = helper.createEmitter(resultTablet);
    }

    public void finalizeGroups() throws IOException {
        emitter.finalizeGroups();
    }

    public InMemoryWriteonlyTablet getResultTablet() {
        return resultTablet;
    }

    public void performQuery(Tablet sourceTablet) throws IOException {
        Slice slice = new Slice(sourceTablet, involvedFields);
        performSelectProjectAggregate(slice, helper, emitter);
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
        if (helper.getOriginalQuery().getWhereClauses().size() <= 0) {
            return true;
        }
        for (SchemaNode schema : selectedFields) {
            if (slice.getColumn(schema).isNull()) {
                continue;
            }
            for (WhereClause clause : helper.getOriginalQuery()
                    .getWhereClauses()) {
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

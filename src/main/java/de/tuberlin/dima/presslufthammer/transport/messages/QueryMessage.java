package de.tuberlin.dima.presslufthammer.transport.messages;

import de.tuberlin.dima.presslufthammer.query.Query;

public class QueryMessage {
    private final int queryId;
    private final Query query;

    public QueryMessage(int queryId, Query query) {
        this.query = query;
        this.queryId = queryId;
    }

    public Query getQuery() {
        return query;
    }

    public int getQueryId() {
        return queryId;
    }
}

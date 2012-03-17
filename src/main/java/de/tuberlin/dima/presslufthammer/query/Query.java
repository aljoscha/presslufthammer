package de.tuberlin.dima.presslufthammer.query;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * A class for representing a query in our simplistic query language. The
 * toString() method generates output that can again be parser by our query
 * parser, this is useful for transmitting the query over the network. (Too lazy
 * to implement a proper serialized form ... :D)
 * 
 * @author Aljoscha Krettek
 * 
 */
public class Query {
    private List<SelectClause> selectClauses;
    private List<WhereClause> whereClauses;
    private List<String> groupByColumns;
    private String tableName;
    private int partition;

    public Query(String tableName, int partition,
            List<SelectClause> selectClauses, List<WhereClause> whereClauses,
            List<String> groupByColumns) {
        this.partition = partition;
        this.selectClauses = Lists.newLinkedList(selectClauses);
        this.whereClauses = Lists.newLinkedList(whereClauses);
        this.groupByColumns = Lists.newLinkedList(groupByColumns);
        this.tableName = tableName;
    }

    public Query(Query other) {
        this.partition = other.partition;
        this.selectClauses = Lists.newLinkedList(other.selectClauses);
        this.whereClauses = Lists.newLinkedList(other.whereClauses);
        this.groupByColumns = Lists.newLinkedList(other.groupByColumns);
        this.tableName = other.tableName;
    }

    public List<SelectClause> getSelectClauses() {
        return selectClauses;
    }

    public void setSelectClauses(List<SelectClause> selectClauses) {
        this.selectClauses = selectClauses;
    }

    public List<WhereClause> getWhereClauses() {
        return whereClauses;
    }

    public void setWhereClauses(List<WhereClause> whereClauses) {
        this.whereClauses = whereClauses;
    }

    public List<String> getGroupByColumns() {
        return groupByColumns;
    }

    public void setGroupByColumns(List<String> groupByColumns) {
        this.groupByColumns = groupByColumns;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }
    
    /**
     * @return byte representation of the query for network transfer
     */
    public byte[] getBytes() {
    	return toString().getBytes();
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("SELECT ");
        Joiner commaJoiner = Joiner.on(",");
        result.append(commaJoiner.join(selectClauses));
        result.append(" FROM " + tableName + ":" + partition);

        Joiner orJoiner = Joiner.on(" OR ");
        if (whereClauses.size() > 0) {
            result.append(" WHERE ");
            result.append(orJoiner.join(whereClauses));
        }

        if (groupByColumns.size() > 0) {
            result.append(" GROUP BY ");
            result.append(commaJoiner.join(groupByColumns));
        }

        return result.toString();
    }
}

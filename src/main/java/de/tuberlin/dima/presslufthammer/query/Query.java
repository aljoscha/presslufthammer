package de.tuberlin.dima.presslufthammer.query;

import java.util.Iterator;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * @author feichh
 * @author Aljoscha Krettek
 * 
 */
public class Query {
    private static final String SEPERATOR = ":";
    private static final String SUBSEPERATOR = ",";
    private static final Splitter splitter = Splitter.on(SEPERATOR);
    private static final Splitter subSplitter = Splitter.on(SUBSEPERATOR);

    private byte id;
    private Projection[] select;
    private String from;
    private byte part;
    private Selection where;
    private String[] groupBy;

    /**
     * empty constructor
     */
    public Query() {
    }

    /**
     * Constructor with fields
     * 
     * @param id
     *            query ID
     * @param select
     *            projection clause
     * @param from
     *            targeted data source (table)
     * @param part
     *            partition of the source
     * @param where
     *            selection clause
     * @param groupBy
     *            grouping clause
     */
    public Query(byte id, Projection[] select, String from, byte part,
            Selection where, String[] groupBy) {
        super();
        assert (from != null);
        this.id = id;
        this.select = select;
        this.from = from;
        this.part = part;
        this.where = where;
        this.groupBy = groupBy;
    }

    /**
     * expects Query.toString().getBytes() as input
     * 
     * @param bytes
     *            bytes of a String representing a Query
     */
    public Query(byte[] bytes) {
        this(new String(bytes));
    }

    /**
     * expects Query.toString() as input
     * 
     * @param query
     *            String representation of a Query
     */
    public Query(String query) {
        Iterator<String> splitIter = splitter.split(query).iterator();
        // ID
        this.id = Byte.parseByte(splitIter.next());
        // SELECT
        Iterable<String> selectIter = subSplitter.split(splitIter.next());
        List<Projection> selectList = Lists.newLinkedList();
        for (String select : selectIter) {
            selectList.add(new Projection(select));
        }
        this.select = selectList.toArray(new Projection[selectList.size()]);
        // FROM
        this.from = splitIter.next();
        // PART
        this.part = Byte.parseByte(splitIter.next());
        // WHERE
        this.where = new Selection(splitIter.next());
        // GROUP BY
        Iterable<String> groupIter = subSplitter.split(splitIter.next());
        List<String> groupByList = Lists.newLinkedList();
        for (String groupBy : groupIter) {
            groupByList.add(groupBy);
        }
        this.groupBy = groupByList.toArray(new String[groupByList.size()]);
    }

    /**
     * @return the id
     */
    public byte getId() {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(byte id) {
        this.id = id;
    }

    /**
     * @return the select
     */
    public Projection[] getSelect() {
        return select;
    }

    /**
     * @param select
     *            the select to set
     */
    public void setSelect(Projection[] select) {
        this.select = select;
    }

    /**
     * @return the from
     */
    public String getFrom() {
        return from;
    }

    /**
     * @param from
     *            the from to set
     */
    public void setFrom(String from) {
        this.from = from;
    }

    /**
     * @return the part
     */
    public byte getPart() {
        return part;
    }

    /**
     * @param part
     *            the part to set
     */
    public void setPart(byte part) {
        this.part = part;
    }

    /**
     * @return the where
     */
    public Selection getWhere() {
        return where;
    }

    /**
     * @param where
     *            the where to set
     */
    public void setWhere(Selection where) {
        this.where = where;
    }

    /**
     * @return the groupBy
     */
    public String[] getGroupBy() {
        return groupBy;
    }

    /**
     * @param groupBy
     *            the groupBy to set
     */
    public void setGroupBy(String[] groupBy) {
        this.groupBy = groupBy;
    }
    
    /**
     * @return byte representation of the query for network transfer
     */
    public byte[] getBytes() {
    	return toString().getBytes();
    }

    @Override
    public String toString() {
        String string = id + SEPERATOR;
        Joiner subJoin = Joiner.on(SUBSEPERATOR);
        string += subJoin.join(select);
        string += SEPERATOR + from;
        string += SEPERATOR + part;
        string += SEPERATOR + where + SEPERATOR;
        string += subJoin.join(groupBy);
        return string;
    }
}

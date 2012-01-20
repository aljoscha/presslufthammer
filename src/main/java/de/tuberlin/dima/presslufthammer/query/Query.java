/**
 * 
 */
package de.tuberlin.dima.presslufthammer.query;

/**
 * @author feichh
 * 
 */
public class Query {
	private static final String SEPERATOR = ":";
	private static final String SUBSEPERATOR = ",";

	private byte id;
	private Projection[] select;
	private String from;
	private byte part;
	private Selection where;
	private String[] groupBy;

	/**
	 * 
	 */
	public Query() {
	}

	/**
	 * @param id
	 * @param select
	 * @param from
	 * @param part
	 * @param where
	 * @param groupBy
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		// TODO
		String string = id + SEPERATOR;
		for (Projection p : select) {
			string += p.toString() + SUBSEPERATOR;
		}
		string += SEPERATOR + from;
		string += SEPERATOR + part;
		string += SEPERATOR + where + SEPERATOR;
		for (String g : groupBy) {
			string += g + SUBSEPERATOR;
		}
		return super.toString();
	}
}

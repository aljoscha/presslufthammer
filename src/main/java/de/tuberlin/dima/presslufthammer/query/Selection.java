package de.tuberlin.dima.presslufthammer.query;

public class Selection {
	private static final String SEPERATOR = " ";
	private String column;
	private Operand op;
	private String filter;

	public Selection(String column, Operand op, String filter) {
		super();
		this.setColumn(column);
		this.setOp(op);
		this.setFilter(filter);
	}

	public Selection(String equivalent) {
		assert (equivalent != null && equivalent.length() > 0);
		String[] split = equivalent.split(SEPERATOR);
		column = split[0];

		if (split.length > 1) {
			op = Operand.valueOf(split[1]);
		}

		if (split.length > 2) {
			filter = split[2];
		}
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public Operand getOp() {
		return op;
	}

	public void setOp(Operand op) {
		this.op = op;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		if (op != null) {
			return column + SEPERATOR + op.name() + SEPERATOR + filter;
		} else {
			return column;
		}
	}
}
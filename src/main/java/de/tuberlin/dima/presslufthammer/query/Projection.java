package de.tuberlin.dima.presslufthammer.query;

public class Projection {
	private String column;

	// private String rename;

	public Projection(String column) {
		super();
		this.column = column;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public byte[] getBytes() {
		// TODO Auto-generated method stub

		return column.getBytes();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {

		return column;
	}
}
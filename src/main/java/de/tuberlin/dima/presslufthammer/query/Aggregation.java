package de.tuberlin.dima.presslufthammer.query;

/**
 * @author feichh
 *
 */
public class Aggregation extends Projection {
	
	private String function;
	
	public Aggregation(String column) {
		super(column);
	}

	public String getFunction() {
		return function;
	}

	public void setFunction(String function) {
		this.function = function;
	}
}
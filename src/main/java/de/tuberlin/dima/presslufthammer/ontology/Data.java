package de.tuberlin.dima.presslufthammer.ontology;

/**
 * resposnse to an query
 */
public class Data {
	
	private long id;
	
	// TODO for testing the network the response is just a string with the name of the answered nodes
	private String value;
	
	public Data(long id, String value) {
		this.id = id;
		this.value = value;
	}
	
	public long getId() {
		return id;
	}
	
	public String getValue() {
		return value;
	}
}
package de.tuberlin.dima.presslufthammer.ontology;

/**
 * a query representation for sending in network
 */
public class Query {
	
	private long id;
	
	// TODO for testing the network there is no content yet
	
	public Query(long id) {
		this.id = id;
	}
	
	public long getId() {
		return id;
	}
}
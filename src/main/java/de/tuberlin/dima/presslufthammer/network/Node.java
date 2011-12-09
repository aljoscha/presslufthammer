package de.tuberlin.dima.presslufthammer.network;

import org.apache.log4j.Logger;
import de.tuberlin.dima.presslufthammer.ontology.Data;
import de.tuberlin.dima.presslufthammer.ontology.Query;

public abstract class Node {
	
	protected final Logger logger;
	protected final int port;
	// just to identify multiple nodes running on the same machine
	protected final String name;
	
	public Node(String name, int port) {
		logger = Logger.getLogger(name);
		this.name = name;
		this.port = port;
	}
	
	abstract public Data answer(Query q);
}
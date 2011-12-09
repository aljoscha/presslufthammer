package de.tuberlin.dima.presslufthammer.netword;

import org.apache.log4j.Logger;

public abstract class Node {
	
	protected final Logger logger;
	protected final int port;

	public Node(String name, int port){
		logger = Logger.getLogger(name);
		this.port = port;
	}
}
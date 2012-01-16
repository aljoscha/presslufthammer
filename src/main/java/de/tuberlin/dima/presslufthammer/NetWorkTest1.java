package de.tuberlin.dima.presslufthammer;

import org.apache.log4j.BasicConfigurator;

import de.tuberlin.dima.presslufthammer.network.LeafNode;
import de.tuberlin.dima.presslufthammer.network.RootNode;
import de.tuberlin.dima.presslufthammer.ontology.Query;

public class NetWorkTest1 {
	private static final String HOSTNAME = "localhost";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// setup logger
		BasicConfigurator.configure();
		
		// setup network
		RootNode root = new RootNode("Jacqueline", 6001);
		LeafNode patty = new LeafNode("Patty", 8001);
		
		root.addChildNode(HOSTNAME, 8001);
		
		patty.setParentNode(HOSTNAME, 6001);
		
		root.handleQuery(new Query(1));
		root.handleQuery(new Query(2));
		root.handleQuery(new Query(3));
		root.handleQuery(new Query(4));
	}	
}
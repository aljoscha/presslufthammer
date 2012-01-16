package de.tuberlin.dima.presslufthammer;

import org.apache.log4j.BasicConfigurator;

import de.tuberlin.dima.presslufthammer.network.LeafNode;
import de.tuberlin.dima.presslufthammer.network.Node;
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
		final RootNode root = new RootNode("Jacqueline", HOSTNAME, 6001);
		final LeafNode patty = new LeafNode("Patty", HOSTNAME, 8001);
		
		root.addChildNode(HOSTNAME, 8001);
		
		patty.setParentNode(HOSTNAME, 6001);
		
		start(patty);
		start(root);
		
		root.handleQuery(new Query(1));
		root.handleQuery(new Query(2));
		root.handleQuery(new Query(3));
		root.handleQuery(new Query(4));
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			
			@Override
			public void run() {
				root.stop();
				patty.stop();
			}
		});
	}
	
	public static void start(Node n){
		if(!n.start()){
			System.exit(-1);
		}
	}
}
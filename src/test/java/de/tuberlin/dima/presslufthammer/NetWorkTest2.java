package de.tuberlin.dima.presslufthammer;

import org.apache.log4j.BasicConfigurator;

import de.tuberlin.dima.presslufthammer.network.InnerNode;
import de.tuberlin.dima.presslufthammer.network.LeafNode;
import de.tuberlin.dima.presslufthammer.network.Node;
import de.tuberlin.dima.presslufthammer.network.RootNode;
import de.tuberlin.dima.presslufthammer.ontology.Query;

public class NetWorkTest2 {
	
	private static final String HOSTNAME = "localhost";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// setup logger
		BasicConfigurator.configure();
		
		// setup network
		final RootNode root = new RootNode("Jacqueline", HOSTNAME, 6001);
		
		final InnerNode selma = new InnerNode("Selma", HOSTNAME, 7001);
		final InnerNode marge = new InnerNode("Marge", HOSTNAME, 7002);
		
		final LeafNode patty = new LeafNode("Patty", HOSTNAME, 8001);
		
		final LeafNode bart = new LeafNode("Bart", HOSTNAME, 8002);
		final LeafNode lisa = new LeafNode("Lisa", HOSTNAME, 8003);
		final LeafNode maggy = new LeafNode("Maggy", HOSTNAME, 8004);
		
		final LeafNode ling = new LeafNode("Ling", HOSTNAME, 8005);
		
		// set children of Jacqueline
		root.addChildNode(HOSTNAME, 7001);
		root.addChildNode(HOSTNAME, 7002);
		root.addChildNode(HOSTNAME, 8001);
		
		// set Jacqueline as parent
		marge.setParentNode(HOSTNAME, 6001);
		selma.setParentNode(HOSTNAME, 6001);
		patty.setParentNode(HOSTNAME, 6001);
		
		// set child of Selma
		selma.addChildNode(HOSTNAME, 8005);
		
		// set Selma as parent
		ling.setParentNode(HOSTNAME, 7001);
		
		// set children of Marge
		marge.addChildNode(HOSTNAME, 8002);
		marge.addChildNode(HOSTNAME, 8003);
		marge.addChildNode(HOSTNAME, 8004);
		
		// set Marge as parent
		bart.setParentNode(HOSTNAME, 7002);
		lisa.setParentNode(HOSTNAME, 7002);
		maggy.setParentNode(HOSTNAME, 7002);
		
		start(bart);
		start(lisa);
		start(maggy);
		
		start(marge);
		
		start(ling);
		
		start(selma);
		
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
				selma.stop();
				ling.stop();
				marge.stop();
				bart.stop();
				lisa.stop();
				maggy.stop();
			}
		});
	}
	
	public static void start(Node n) {
		if (!n.start()) {
			System.exit(-1);
		}
	}
}
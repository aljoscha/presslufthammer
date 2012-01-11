package de.tuberlin.dima.presslufthammer;

import java.io.IOException;

import org.apache.log4j.BasicConfigurator;

import de.tuberlin.dima.presslufthammer.network.InnerNode;
import de.tuberlin.dima.presslufthammer.network.LeafNode;
import de.tuberlin.dima.presslufthammer.network.RootNode;
import de.tuberlin.dima.presslufthammer.ontology.Query;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class NetWorkTest2 extends TestCase {
	
	private static final String HOSTNAME = "localhost";
	
	public NetWorkTest2(String testName) {
		super(testName);
	}
	
	public static Test suite() {
		return new TestSuite(NetWorkTest2.class);
	}
	
	public void testNetWork() {
		// setup logger
		BasicConfigurator.configure();
		
		// setup network
		RootNode root = new RootNode("Jacqueline", 6001);
		
//		InnerNode selma = new InnerNode("Selma", 7001);
//		InnerNode marge = new InnerNode("Marge", 7002);
		
		LeafNode patty = new LeafNode("Patty", 8001);
		
//		LeafNode bart = new LeafNode("Bart", 8002);
//		LeafNode lisa = new LeafNode("Lisa", 8003);
//		LeafNode maggy = new LeafNode("Maggy", 8004);
//		
//		LeafNode ling = new LeafNode("Ling", 8005);
//		
		// set children of Jacqueline
//		root.addChildNode(HOSTNAME, 7001);
//		root.addChildNode(HOSTNAME, 7002);
		root.addChildNode(HOSTNAME, 8001);
		
		// set Jacqueline as parent
//		marge.setParentNode(HOSTNAME, 6001);
//		selma.setParentNode(HOSTNAME, 6001);
		patty.setParentNode(HOSTNAME, 6001);
		
		// set child of Selma
//		selma.addChildNode(HOSTNAME, 8005);
		
		// set Selma as parent
//		ling.setParentNode(HOSTNAME, 7001);
		
		// set children of Marge
//		marge.addChildNode(HOSTNAME, 8002);
//		marge.addChildNode(HOSTNAME, 8003);
//		marge.addChildNode(HOSTNAME, 8004);
		
		// set Marge as parent
//		bart.setParentNode(HOSTNAME, 7002);
//		lisa.setParentNode(HOSTNAME, 7002);
//		maggy.setParentNode(HOSTNAME, 7002);
		
		root.handleQuery(new Query(1));
		root.handleQuery(new Query(2));
		root.handleQuery(new Query(3));
		root.handleQuery(new Query(4));
		
		
		
//		try {
//			root.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		try {
//			patty.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
}
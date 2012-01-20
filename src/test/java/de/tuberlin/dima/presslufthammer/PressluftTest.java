package de.tuberlin.dima.presslufthammer;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import de.tuberlin.dima.presslufthammer.transport.CLIClient;
import de.tuberlin.dima.presslufthammer.transport.Coordinator;
import de.tuberlin.dima.presslufthammer.transport.Inner;
import de.tuberlin.dima.presslufthammer.transport.Leaf;

/**
 * Unit test for simple App.
 */
public class PressluftTest extends TestCase {
	private static final String HOST = "localhost";
	private static final int HOSTPORT = 44444;
	
	/**
	 * Create the test case
	 * 
	 * @param testName
	 *            name of the test case
	 */
	public PressluftTest(String testName) {
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite(PressluftTest.class);
	}

	/**
	 * Rigorous Test :-)
	 * @throws Exception 
	 */
	public void testOneOfEach() throws Exception {

		Coordinator coord = new Coordinator(HOSTPORT, "DataSources.xml");
		coord.start();
		Inner inner = new Inner(HOST, HOSTPORT);
		Leaf leaf = new Leaf(HOST, HOSTPORT);
		leaf.start();
		
		CLIClient.main(new String[] { HOST, String.valueOf( HOSTPORT)});
		
		leaf.stop();
		inner.close();
		coord.close();
	}
	
	/**
	 * Rigorous Test :-)
	 * @throws Exception 
	 */
	public void testThreeLeafs() throws Exception {
		
		Coordinator coord = new Coordinator(HOSTPORT, "DataSources.xml");
		coord.start();
		Leaf leaf1 = new Leaf(HOST, HOSTPORT);
		Leaf leaf2 = new Leaf(HOST, HOSTPORT);
		Leaf leaf3 = new Leaf(HOST, HOSTPORT);
		leaf1.start();
		leaf2.start();
		leaf3.start();
		
		CLIClient.main(new String[] { HOST, String.valueOf( HOSTPORT)});
		
		leaf1.stop();
		leaf2.stop();
		leaf3.stop();
		coord.close();
	}
}

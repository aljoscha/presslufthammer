package de.tuberlin.dima.presslufthammer;

import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.BasicConfigurator;

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
	private static final String DATASRCS = "DataSources.xml";
	
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
		BasicConfigurator.configure();

		Coordinator coord = new Coordinator(HOSTPORT, DATASRCS);
		Inner inner = new Inner(HOST, HOSTPORT);
		Leaf leaf = new Leaf(HOST, HOSTPORT);
		
		CLIClient.main(new String[] { HOST, String.valueOf( HOSTPORT)});
		
		leaf.close();
		inner.close();
		coord.close();
	}
	
	/**
	 * Rigorous Test :-)
	 * @throws Exception 
	 */
	public void testThreeLeafs() throws Exception {
		BasicConfigurator.configure();
		
		Coordinator coord = new Coordinator(HOSTPORT, DATASRCS);
		Leaf leaf1 = new Leaf(HOST, HOSTPORT);
		Leaf leaf2 = new Leaf(HOST, HOSTPORT);
		Leaf leaf3 = new Leaf(HOST, HOSTPORT);
		
		CLIClient.main(new String[] { HOST, String.valueOf( HOSTPORT)});
		
		leaf1.close();
		leaf2.close();
		leaf3.close();
		coord.close();
	}
}

package de.tuberlin.dima.presslufthammer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.BasicConfigurator;

import de.tuberlin.dima.presslufthammer.transport.CLIClient;
import de.tuberlin.dima.presslufthammer.transport.Coordinator;
import de.tuberlin.dima.presslufthammer.transport.Inner;
import de.tuberlin.dima.presslufthammer.transport.Leaf;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
	 * 
	 * @throws IOException
	 */
	public void testOneOfEach() throws IOException {
		BasicConfigurator.configure();


		Coordinator coord = new Coordinator(HOSTPORT);

		Inner inner = new Inner(HOST, HOSTPORT);

		Leaf leaf = new Leaf(HOST, HOSTPORT);

		CLIClient client = new CLIClient(HOST, HOSTPORT);
		boolean assange = true;
		// Console console = System.console();
		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(System.in));
		while (assange) {
			String line = bufferedReader.readLine();
			if (line.startsWith("x")) {
				assange = false;
			} else {
				client.sendQuery(line);
			}
		}

		client.close();
		leaf.close();
		inner.close();
		coord.close();
	}
	
	public void testThreeLeafs() throws IOException {
		BasicConfigurator.configure();
		
		Coordinator coord = new Coordinator(HOSTPORT);
		Leaf leaf1 = new Leaf(HOST, HOSTPORT);
		Leaf leaf2 = new Leaf(HOST, HOSTPORT);
		Leaf leaf3 = new Leaf(HOST, HOSTPORT);
		CLIClient client = new CLIClient(HOST, HOSTPORT);

		boolean assange = true;
		// Console console = System.console();
		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(System.in));
		while (assange) {
			String line = bufferedReader.readLine();
			if (line.startsWith("x")) {
				assange = false;
			} else {
				client.sendQuery(line);
			}
		}
		
		client.close();
		leaf1.close();
		leaf2.close();
		leaf3.close();
		coord.close();
	}
}

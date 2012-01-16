package de.tuberlin.dima.presslufthammer;

import java.io.IOException;

import org.apache.log4j.BasicConfigurator;

import de.tuberlin.dima.presslufthammer.transport.CLIClient;
import de.tuberlin.dima.presslufthammer.transport.Coordinator;
import de.tuberlin.dima.presslufthammer.transport.Leaf;

public class PressLuftTestThreeLeafs {
	
	private static final String HOST = "localhost";
	private static final int HOSTPORT = 44444;
	
	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		BasicConfigurator.configure();
		
		Coordinator coord = new Coordinator(HOSTPORT);
		Leaf leaf1 = new Leaf(HOST, HOSTPORT);
		Leaf leaf2 = new Leaf(HOST, HOSTPORT);
		Leaf leaf3 = new Leaf(HOST, HOSTPORT);
		
		CLIClient.main(new String[] { HOST, String.valueOf(HOSTPORT) });
		
		leaf1.close();
		leaf2.close();
		leaf3.close();
		coord.close();
	}	
}
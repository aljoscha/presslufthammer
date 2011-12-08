package de.tuberlin.dima.presslufthammer.testing;

import org.apache.log4j.BasicConfigurator;

/**
 * Hello world!
 * 
 */
public class RunCoordinator
{
	/**
	 * Prints the usage to System.out.
	 */
	private static void printUsage()
	{
		// TODO Auto-generated method stub
		System.out.println( "Parameters:");
		System.out.println( "port");
	}

	public static void main( String[] args)
	{
		// System.out.println( "Hello World!" );
		// Print usage if necessary.
		if( args.length < 1)
		{
			printUsage();
			return;
		}
		BasicConfigurator.configure();

		int port = Integer.parseInt( args[0]);

		Coordinator coord = new Coordinator( port);
	}
}

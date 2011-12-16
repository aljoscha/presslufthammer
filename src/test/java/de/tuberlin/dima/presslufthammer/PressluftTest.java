package de.tuberlin.dima.presslufthammer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.BasicConfigurator;

import de.tuberlin.dima.presslufthammer.testing.CLIClient;
import de.tuberlin.dima.presslufthammer.testing.Coordinator;
import de.tuberlin.dima.presslufthammer.testing.Inner;
import de.tuberlin.dima.presslufthammer.testing.Leaf;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class PressluftTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public PressluftTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( PressluftTest.class );
    }

    /**
     * Rigorous Test :-)
     * @throws IOException 
     */
    public void testApp() throws IOException
    {
    	BasicConfigurator.configure();
    	
    	String host = "localhost";
    	int port = 44444;
    	
    	Coordinator coord = new Coordinator( port);
    	
  		Inner inner = new Inner( host, port);
  		
  		Leaf leaf = new Leaf( host, port);
    	
    	CLIClient client = new CLIClient( host, port);
  		boolean assange = true;
//  		Console console = System.console();
  		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
  		while( assange)
  		{
  			String line = bufferedReader.readLine();
  			if( line.startsWith( "x"))
  			{
  				assange = false;
  			}
  			else {
  				client.sendQuery( line);
  			}
  		}
  		
  		client.close();
  		leaf.close();
  		inner.close();
  		coord.close();
    }
}

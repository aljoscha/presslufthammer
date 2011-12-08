/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

/**
 * @author feichh
 *
 */
public interface Serving
{
	public boolean serve( String host, int port);
	
	public boolean serve( int port);
}

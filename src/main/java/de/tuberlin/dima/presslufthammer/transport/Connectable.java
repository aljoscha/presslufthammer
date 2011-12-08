/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

/**
 * @author feichh
 *
 */
public interface Connectable
{
	public boolean connect( String host, int port);
	
	public boolean send( Object msg);
}

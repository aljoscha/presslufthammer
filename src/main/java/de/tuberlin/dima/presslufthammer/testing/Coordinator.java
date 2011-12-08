/**
 * 
 */
package de.tuberlin.dima.presslufthammer.testing;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * @author feichh
 * 
 */
public class Coordinator
{

	private final Logger		logg					= Logger.getLogger( getClass());

	protected ChannelGroup	openChannels	= new DefaultChannelGroup();
	private CoordinatorHandler handler = new CoordinatorHandler( openChannels);

	/**
	 * @param port
	 */
	public Coordinator( int port)
	{
		// Configure the server.
		ServerBootstrap bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory( Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		// Set up the event pipeline factory.
		bootstrap.setPipelineFactory( new CoordinatorPipelineFac( this.openChannels, this.handler));

		// Bind and start to accept incoming connections.
		bootstrap.bind( new InetSocketAddress( port));
		logg.debug( "Coordinator launched at: " + port);

	}

}

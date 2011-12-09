/**
 * 
 */
package de.tuberlin.dima.presslufthammer.testing;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author feichh
 * 
 */
public class Coordinator extends ChannelNode
{

	private final Logger				log					= LoggerFactory.getLogger( getClass());

	protected ChannelGroup			innerChannels	= new DefaultChannelGroup();
	protected ChannelGroup			leafChannels	= new DefaultChannelGroup();
	private CoordinatorHandler	handler				= new CoordinatorHandler( this);
	private Channel							innerRootChan;

	/**
	 * @param port
	 */
	public Coordinator( int port)
	{
		// TODO
		// Configure the server.
		ServerBootstrap bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory( Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		// Set up the event pipeline factory.
		bootstrap.setPipelineFactory( new CoordinatorPipelineFac(
				this.openChannels, this.handler));

		// Bind and start to accept incoming connections.
		bootstrap.bind( new InetSocketAddress( port));
		log.debug( "Coordinator launched at: " + port);

	}

	/**
	 * @param query
	 */
	public void query( String query)
	{
		// TODO
		log.debug( "query()");
		if( handler != null)
		{

		}
	}

	/**
	 * @param channel
	 * @param remoteAddress
	 */
	public void addInner( Channel channel, SocketAddress remoteAddress)
	{
		// TODO
		log.debug( "adding inner channel: " + remoteAddress);
		innerChannels.add( channel);
		if( innerRootChan == null)
		{
			innerRootChan = channel;
		}
	}

	/**
	 * @param channel
	 * @param remoteAddress
	 */
	public void addLeaf( Channel channel, SocketAddress remoteAddress)
	{
		// TODO
		log.debug( "adding leaf channel: " + remoteAddress);
		leafChannels.add( channel);
	}
}

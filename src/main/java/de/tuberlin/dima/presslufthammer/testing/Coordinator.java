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

import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;
import de.tuberlin.dima.presslufthammer.pressluft.Type;

/**
 * @author feichh
 * 
 */
public class Coordinator extends ChannelNode
{

	private final Logger				log					= LoggerFactory
																							.getLogger( getClass());
	ChannelGroup								innerChans	= new DefaultChannelGroup();
	ChannelGroup								leafChans		= new DefaultChannelGroup();
	private CoordinatorHandler	handler			= new CoordinatorHandler( this);
	private Channel							rootChan;

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
		bootstrap.setPipelineFactory( new CoordinatorPipelineFac( this.handler));

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
		assert isServing();
		if( handler != null)
		{
			Pressluft queryMSG = getQMSG( query);
			rootChan.write( queryMSG);
		}
	}

	/**
	 * @param query
	 * @return
	 */
	private Pressluft getQMSG( String query)
	{
		// TODO
		Type type = Type.QUERY;
		byte[] payload = query.getBytes();

		return new Pressluft( type, payload);
	}

	public boolean isServing()
	{
		return !(innerChans.isEmpty() || leafChans.isEmpty());
	}

	/**
	 * @param channel
	 * @param remoteAddress
	 */
	public void addInner( Channel channel, SocketAddress remoteAddress)
	{
		// TODO
		log.debug( "adding inner channel: " + remoteAddress);
		innerChans.add( channel);
		if( rootChan == null)
		{
			rootChan = channel;
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
		leafChans.add( channel);
		channel.write( getRootInfo());
	}

	/**
	 * @return
	 */
	private Pressluft getRootInfo()
	{
		// TODO
		Type type = Type.INFO;
		byte[] payload = rootChan.getRemoteAddress().toString().getBytes();
		return new Pressluft( type, payload);
	}
}

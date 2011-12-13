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
	ChannelGroup								clientChans	= new DefaultChannelGroup();
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
	public void query( Pressluft query)
	{
		// TODO
		log.debug( "query()");
		assert isServing();
		if( handler != null && rootChan != null)
		{
//			Pressluft queryMSG = getQMSG( query);
//			rootChan.write( queryMSG);
			rootChan.write( query);
		}
	}

	/**
	 * @return true if this coordinator is connected to at least 1 Inner and 1 Leaf
	 */
	public boolean isServing()
	{
		return !(innerChans.isEmpty() || leafChans.isEmpty());
	}

	/**
	 * @param channel
	 */
	public void addClient( Channel channel)
	{
		// TODO Auto-generated method stub
		log.info( "adding client channel: " + channel.getRemoteAddress());
		clientChans.add( channel);
	}

	/**
	 * @param channel
	 * @param address
	 */
	public void addInner( Channel channel)
	{
		// TODO
		log.info( "adding inner channel: " + channel.getRemoteAddress());
		innerChans.add( channel);
		if( rootChan == null)
		{
			log.debug( "new root node found.");
			rootChan = channel;
		}
	}
	
	/**
	 * @param channel
	 * @param address
	 */
	public void addLeaf( Channel channel)
	{
		// TODO
		log.debug( "adding leaf channel: " + channel.getRemoteAddress());
		leafChans.add( channel);
		if( rootChan != null) {
			channel.write( getRootInfo());
		}
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

	/**
	 * @param channel
	 */
	public void removeChannel( Channel channel)
	{
		// TODO
		if( rootChan == channel) {
			rootChan = null;
		}
		channel.close();
//		log.debug( "" + openChannels.remove( channel));
	}
	
	
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

		int port = Integer.parseInt( args[0]);

		Coordinator coord = new Coordinator( port);
	}
}

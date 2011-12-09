/**
 * 
 */
package de.tuberlin.dima.presslufthammer.testing;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;
import de.tuberlin.dima.presslufthammer.pressluft.Type;

/**
 * @author feichh
 * 
 */
public class Inner extends ChannelNode
{
	private static final Pressluft REGMSG = new Pressluft( Type.REGINNER, new byte[] { (byte) 0,});
	private final Logger	log					= LoggerFactory.getLogger( getClass());

	ChannelGroup					childChannels	= new DefaultChannelGroup();
	Channel								coordChan, parentChan;

	/**
	 * @param host
	 * @param port
	 * @throws InterruptedException if interrupted
	 */
	public Inner( String host, int port)
	{
		// Configure the server.
		ServerBootstrap bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory( Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		// Set up the event pipeline factory.
		bootstrap.setPipelineFactory( new InnerPipelineFac( this));

		// Bind and start to accept incoming connections.
		bootstrap.bind( new InetSocketAddress( port + 1));
		
		if( connectNReg( host, port))
		{
			log.info( "connected");
		}
//
//		channel.close().awaitUninterruptibly();
//		bootstrap.releaseExternalResources();
	}

	/**
	 * @param host
	 * @param port
	 * @return true if connection attempt was successful
	 */
	public boolean connectNReg( String host, int port)
	{
		return connectNReg( new InetSocketAddress( host, port));
	}

	/**
	 * @param innerAddress
	 */
	public boolean connectNReg( SocketAddress address)
	{
		// TODO
		ClientBootstrap bootstrap = new ClientBootstrap(
				new NioClientSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory( new InnerPipelineFac( this));

		ChannelFuture connectFuture = bootstrap.connect( address);

		coordChan = connectFuture.awaitUninterruptibly().getChannel();
		ChannelFuture writeFuture = coordChan.write( REGMSG);

		return writeFuture.awaitUninterruptibly().isSuccess();
	}

	/**
	 * @param channel
	 */
	public void regChild( Channel channel)
	{
		// TODO Auto-generated method stub
		childChannels.add( channel);
	}

	public static void main( String[] args) throws Exception
	{

		Inner in = new Inner(  "localhost", 44444);

	}
}

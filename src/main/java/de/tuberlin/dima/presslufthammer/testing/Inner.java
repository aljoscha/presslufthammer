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
import org.jboss.netty.channel.ChannelFutureListener;
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
	private static final Pressluft	REGMSG				= new Pressluft( Type.REGINNER,
																										new byte[] { (byte) 0, });
	private final Logger						log						= LoggerFactory
																										.getLogger( getClass());

	ChannelGroup										childChannels	= new DefaultChannelGroup();
	Channel													coordChan, parentChan;

	/**
	 * @param host
	 * @param port
	 * @throws InterruptedException
	 *           if interrupted
	 */
	public Inner( String host, int port)
	{

		if( connectNReg( host, port))
		{
			log.info( "connected");

			port = getPortFromSocketAddress( coordChan.getLocalAddress()) + 1;
			// Configure the server.
			ServerBootstrap bootstrap = new ServerBootstrap(
					new NioServerSocketChannelFactory( Executors.newCachedThreadPool(),
							Executors.newCachedThreadPool()));

			// Set up the event pipeline factory.
			bootstrap.setPipelineFactory( new InnerPipelineFac( this));

			// Bind and start to accept incoming connections.
			bootstrap.bind( new InetSocketAddress( port));
			log.info( "serving on port: " + port);
		}
	}

	/**
	 * @param address
	 * @return
	 */
	private int getPortFromSocketAddress( SocketAddress address)
	{
		int result = -1;
		if( address instanceof InetSocketAddress)
		{
			InetSocketAddress tempAddr = (InetSocketAddress) address;
			result = tempAddr.getPort();
		}
		else
		{
			// not guaranteed to work
			String s = address.toString();
			// log.debug( s);
			String[] temp = s.split( ":");
			Integer.parseInt( temp[temp.length - 1]);
		}
		return result;
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
				new NioClientSocketChannelFactory( Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory( new InnerPipelineFac( this));

		ChannelFuture connectFuture = bootstrap.connect( address);
		connectFuture.addListener( new ChannelFutureListener() {
			public void operationComplete( ChannelFuture future)
			{
				// Perform post-closure operation
				coordChan = future.getChannel();
				coordChan.write( REGMSG);
			}
		});
		// parentChannel = connectFuture.awaitUninterruptibly().getChannel();
		// ChannelFuture writeFuture = parentChannel.write( REGMSG);

		return coordChan != null && coordChan.isConnected();
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

		Inner in = new Inner( "localhost", 44444);

	}

	/**
	 * @param prsslft
	 */
	public void handDownQuery( Pressluft prsslft)
	{
		// TODO
		if( !childChannels.isEmpty())
		{
			for( Channel chan : childChannels)
			{
				chan.write( prsslft);
			}
		}
		else
		{
			log.error( "query received, but no children connected.");
		}
	}
}

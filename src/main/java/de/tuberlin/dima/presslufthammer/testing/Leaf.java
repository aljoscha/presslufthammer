/**
 * 
 */
package de.tuberlin.dima.presslufthammer.testing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;

/**
 * @author feichh
 * 
 */
public class Leaf extends ChannelNode
{
	private static final Pressluft	REGMSG	= new Pressluft(
																							de.tuberlin.dima.presslufthammer.pressluft.Type.REGLEAF,
																							new byte[] { (byte) 7 });

	private final Logger						log			= LoggerFactory
																							.getLogger( getClass());
	private Channel									parentChannel;

	public Leaf( String host, int port)
	{

		if( connectNReg( host, port))
		{
			log.info( "registered with coordinator");
		}
	}

	/**
	 * see connectNReg( SocketAddress)
	 * 
	 * @param host
	 * @param port
	 * @return
	 */
	public boolean connectNReg( String host, int port)
	{
		return connectNReg( new InetSocketAddress( host, port));
	}

	/**
	 * @param address
	 * @return true if connection was established successfully
	 */
	public boolean connectNReg( SocketAddress address)
	{
		// TODO
		ClientBootstrap bootstrap = new ClientBootstrap(
				new NioClientSocketChannelFactory( Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory( new LeafPipelineFac( this));

		ChannelFuture connectFuture = bootstrap.connect( address);

		parentChannel = connectFuture.awaitUninterruptibly().getChannel();
		ChannelFuture writeFuture = parentChannel.write( REGMSG);

		return writeFuture.awaitUninterruptibly().isSuccess();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.dima.presslufthammer.testing.ChannelNode#close()
	 */
	@Override
	public void close() throws IOException
	{
		// TODO
		//
		// channel.close().awaitUninterruptibly();
		// bootstrap.releaseExternalResources();
		parentChannel.close();
		super.close();
	}

	/**
	 * Prints the usage to System.out.
	 */
	private static void printUsage()
	{
		System.out.println( "Usage:");
		System.out.println( "hostname port");
	}

	/**
	 * @param args
	 * @throws InterruptedException
	 *           if interrupted
	 */
	public static void main( String[] args) throws InterruptedException
	{
		// Print usage if necessary.
		if( args.length < 2)
		{
			printUsage();
			return;
		}
		// Parse options.
		String host = args[0];
		int port = Integer.parseInt( args[1]);

		Leaf leaf = new Leaf( host, port);
	}
}

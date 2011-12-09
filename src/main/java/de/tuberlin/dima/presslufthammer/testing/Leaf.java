/**
 * 
 */
package de.tuberlin.dima.presslufthammer.testing;

import java.net.InetSocketAddress;
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
	private final Logger log = LoggerFactory.getLogger( getClass());
	private static Channel coordChannel;

	/**
	 * Prints the usage to System.out.
	 */
	private static void printUsage()
	{
		// TODO Auto-generated method stub
		System.out.println( "Usage:");
		System.out.println( "hostname port");
	}

	/**
	 * @param args
	 * @throws InterruptedException if interrupted
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

		ClientBootstrap bootstrap = new ClientBootstrap(
				new NioClientSocketChannelFactory( Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory( new LeafPipelineFac());

		ChannelFuture connectFuture = bootstrap.connect( new InetSocketAddress(
				host, port));

		coordChannel = connectFuture.awaitUninterruptibly().getChannel();
		ChannelFuture writeFuture = coordChannel.write( new Pressluft( de.tuberlin.dima.presslufthammer.pressluft.Type.REGLEAF, new byte[] { (byte) 7, (byte) 3}));
		
		writeFuture.await();
		
		System.out.println( "written");
		
//		
//		channel.close().awaitUninterruptibly();
//		bootstrap.releaseExternalResources();
	}
}

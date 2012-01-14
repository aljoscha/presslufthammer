/**
 * 
 */
package de.tuberlin.dima.presslufthammer.testing;

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
public class Inner extends ChannelNode {
	private static final Pressluft REGMSG = new Pressluft(Type.REGINNER, (byte) 0,
			"Hello".getBytes());
	private final Logger log = LoggerFactory.getLogger(getClass());

	ChannelGroup childChannels = new DefaultChannelGroup();
	Channel coordChan, parentChan;

	/**
	 * @param host
	 * @param port
	 * @throws InterruptedException
	 *             if interrupted
	 */
	public Inner(String host, int port) {

		connectNReg(host, port);
		//
		// channel.close().awaitUninterruptibly();
		// bootstrap.releaseExternalResources();
	}

	/**
	 * 
	 */
	public void serve() {
		int port = getPortFromSocketAddress(coordChan.getLocalAddress()) + 1;
		// Configure the server.
		ServerBootstrap bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		// Set up the event pipeline factory.
		bootstrap.setPipelineFactory(new InnerPipelineFac(this));

		// Bind and start to accept incoming connections.
		bootstrap.bind(new InetSocketAddress(port));
		log.info("serving on port: " + port);
	}

	/**
	 * @param localAddress
	 * @return
	 */
	private int getPortFromSocketAddress(SocketAddress localAddress) {
		String s = localAddress.toString();
		// log.debug( s);
		String[] temp = s.split(":");

		return Integer.parseInt(temp[temp.length - 1]);
	}

	/**
	 * @param host
	 * @param port
	 * @return true if connection attempt was successful
	 */
	public boolean connectNReg(String host, int port) {
		return connectNReg(new InetSocketAddress(host, port));
	}

	/**
	 * @param innerAddress
	 */
	public boolean connectNReg(SocketAddress address) {
		// TODO
		ClientBootstrap bootstrap = new ClientBootstrap(
				new NioClientSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory(new InnerPipelineFac(this));
		ChannelFuture connectFuture = bootstrap.connect(address);

		connectFuture.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {
				coordChan = future.getChannel();
				openChannels.add(coordChan);
				coordChan.write(REGMSG).addListener(
						new ChannelFutureListener() {
							public void operationComplete(ChannelFuture future)
									throws Exception {
								log.debug("registered with coordinator @ "
										+ coordChan.getRemoteAddress());
								serve();
							}
						});
			}
		});
		return true;
	}

	/**
	 * @param channel
	 */
	public void regChild(Channel channel) {
		// TODO
		childChannels.add(channel);
	}

	/**
	 * @param query
	 */
	public void query(Pressluft query) {
		for (Channel c : childChannels) {
			log.debug("querying: " + c.getRemoteAddress());
			c.write(query);
		}
	}

	public void handleResult(Pressluft prsslft) {
		// TODO
		coordChan.write(prsslft);
	}
//
//	public static void main(String[] args) throws Exception {
//
//		Inner in = new Inner("localhost", 44444);
//
//	}
}

/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.Type;

/**
 * @author feichh
 * 
 */
public class Inner extends ChannelNode {
	private static final SimpleMessage REGMSG = new SimpleMessage(
			Type.REGINNER, (byte) 0, "Hello".getBytes());
	private final Logger log = LoggerFactory.getLogger(getClass());

	ChannelGroup childChannels = new DefaultChannelGroup();
	Channel coordChan, parentChan;

	/**
	 * Constructor
	 * 
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
		bootstrap.setPipelineFactory(new GenericPipelineFac(this));

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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.dima.presslufthammer.transport.ChannelNode#connectNReg(java
	 * .net.SocketAddress)
	 */
	@Override
	public boolean connectNReg(SocketAddress address) {
		// TODO
		ClientBootstrap bootstrap = new ClientBootstrap(
				new NioClientSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory(new GenericPipelineFac(this));
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.dima.presslufthammer.transport.ChannelNode#query(de.tuberlin
	 * .dima.presslufthammer.pressluft.Pressluft)
	 */
	@Override
	public void query(SimpleMessage query) {
		for (Channel c : childChannels) {
			log.debug("querying: " + c.getRemoteAddress());
			c.write(query);
		}
	}

	public void handleResult(SimpleMessage prsslft) {
		// TODO
		coordChan.write(prsslft);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {

		if (e.getMessage() instanceof SimpleMessage) {
			SimpleMessage message = (SimpleMessage) e.getMessage();
			log.info(message.getType() + " from "
					+ e.getRemoteAddress().toString());

			switch (message.getType()) {
			case ACK:
				// not used yet
				break;
			case INFO:
				// not used yet
				break;
			case INTERNAL_QUERY:
				// TODO split query and hand parts over to children
				this.query(message);
				break;
			case REGINNER:
				// not used yet
				break;
			case REGLEAF:
				// TODO handle new leaf connection
				openChannels.add(e.getChannel());
				this.regChild(e.getChannel());
				break;
			case INTERNAL_RESULT:
				// TODO accumulate results; combine them; send them to parent;
				this.handleResult(message);
				break;
			case UNKNOWN:
				// not used yet
				break;

			}
		}
	}
}

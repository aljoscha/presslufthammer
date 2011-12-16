package de.tuberlin.dima.presslufthammer.network;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import de.tuberlin.dima.presslufthammer.network.handler.ClientHandler;
import de.tuberlin.dima.presslufthammer.pressluft.Encoder;
import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;

public abstract class Node implements Closeable {
	
	protected final Logger logger;
	protected final int port;
	// just to identify multiple nodes running on the same machine
	protected final String name;
	
	protected ServerBootstrap serverBootstrap;
	protected ClientBootstrap clientBootstrap;
	
	public Node(String name, int port) {
		logger = Logger.getLogger(name);
		this.name = name;
		this.port = port;
		
		ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		clientBootstrap = new ClientBootstrap(factory);
		clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(Encoder.getInstance(), new ClientHandler(logger));
			}
		});
	}
	
	protected static void sendPressLuft(Pressluft p, Channel ch, Logger logger) {
		// TODO check if success and log
		
		if ((ch != null) && ch.isConnected() && ch.isWritable()) {
			ChannelFuture f = ch.write(p).awaitUninterruptibly();
			
			if (f.isSuccess()) {
				logger.trace("send " + p + " to " + ch.getRemoteAddress());
			} else {
				logger.error("could not send " + p + " to " + ch.getRemoteAddress() + " : " + f.getCause());
			}
			
		} else {
			if (ch == null) {
				logger.error("could not sent " + p + " to " + ch.getRemoteAddress() + " : channel is null");
			} else if (ch.isConnected()) {
				logger.error("could not sent " + p + " to " + ch.getRemoteAddress() + " : channel is not connected");
			} else if (ch.isWritable()) {
				logger.error("could not sent " + p + " to " + ch.getRemoteAddress() + " : channel is not writable");
			}
		}
	}
	
	public abstract void close() throws IOException;
}
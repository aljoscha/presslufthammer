package de.tuberlin.dima.presslufthammer.network;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
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

public abstract class Node {
	
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
		
//		ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
//		clientBootstrap = new ClientBootstrap(factory);
//		clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
//			
//			public ChannelPipeline getPipeline() throws Exception {
//				return Channels.pipeline(Encoder.getInstance(), new ClientHandler(logger));
//			}
//		});
		
	}
	
	protected static void sendPressLuft(Pressluft p, InetSocketAddress addr, Logger logger) {
		ClientBootstrap bootstrap = getNewClientBootstrap(logger);
//		ClientBootstrap bootstrap = this.clientBootstrap;
		ChannelFuture future = bootstrap.connect(addr);
		
		if (!future.awaitUninterruptibly().isSuccess()) {
			logger.error("failed to connect with " + addr + "");
			bootstrap.releaseExternalResources();
			return;
		}
		
		if (future.getChannel().isConnected()) {
			future.getChannel().write(p).awaitUninterruptibly();
			logger.trace("send " + p + " to " + addr);
		} else {
			logger.error("channel was already closed");
			logger.error("could not send " + p + " to " + addr);
		}
	}
	
	private static ClientBootstrap getNewClientBootstrap(final Logger logger) {
		ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		ClientBootstrap res = new ClientBootstrap(factory);
		res.setPipelineFactory(new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(Encoder.getInstance(), new ClientHandler(logger));
			}
		});
		
		return res;
	}
	
	private static ClientBootstrap getNewClientBootstrap() {
		ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		ClientBootstrap res = new ClientBootstrap(factory);
		res.setPipelineFactory(new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(Encoder.getInstance(), new ClientHandler());
			}
		});
		
		return res;
	}
}
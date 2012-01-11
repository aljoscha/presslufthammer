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
		
		ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		clientBootstrap = new ClientBootstrap(factory);
		clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(Encoder.getInstance(), new ClientHandler(logger));
			}
		});
	}
	
	protected synchronized void sendPressLuft(Pressluft p, InetSocketAddress addr) {
//		ClientBootstrap bootstrap = getNewClientBootstrap();
		ChannelFuture future = clientBootstrap.connect(addr).awaitUninterruptibly();
//		ChannelFuture future = clientBootstrap.connect(addr);
		
		if (!future.awaitUninterruptibly().isSuccess()) {
			logger.error("failed to connect with " + addr + "");
			clientBootstrap.releaseExternalResources();
			return;
		}
		
		if (future.getChannel().isConnected()) {
//			future.getChannel().write(p).awaitUninterruptibly();
			future.getChannel().write(p);
			logger.trace("send " + p + " to " + addr);
		} else {
			logger.error("channel was already closed");
			logger.error("could not send " + p + " to " + addr);
		}
	}
	
	private synchronized ClientBootstrap getNewClientBootstrap() {
		ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		ClientBootstrap res = new ClientBootstrap(factory);
		res.setPipelineFactory(new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(Encoder.getInstance(), new ClientHandler(logger));
			}
		});
		
		return res;
	}
}
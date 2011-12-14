package de.tuberlin.dima.presslufthammer.network;

import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

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
	}
	
	protected void sendPressLuft(Pressluft p, InetSocketAddress addr) {
		ChannelFuture future = this.clientBootstrap.connect(addr);
		
		if (!future.awaitUninterruptibly().isSuccess()) {
			logger.error("Failed to connect with \"" + addr + "\"");
			this.clientBootstrap.releaseExternalResources();
			return;
		}
		
		if (future.getChannel().isConnected()) {
			future.getChannel().write(p).addListener(ChannelFutureListener.CLOSE);
			logger.trace("sending " + p + " to " + addr);
		} else {
			logger.error("channel was already closed");
			logger.error("could not send " + p + " to " + addr);
		}
		
//		future.addListener(ChannelFutureListener.CLOSE);
	}
}
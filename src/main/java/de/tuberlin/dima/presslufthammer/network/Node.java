package de.tuberlin.dima.presslufthammer.network;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.group.ChannelGroup;

public abstract class Node {
	
	// logger
	protected final Logger logger;
	
	// configuration
	protected final String host;
	protected final int port;
	
	// just to identify multiple nodes running on the same machine
	protected final String name;
	
	protected ChannelGroup channelGroup;
	
	protected Node(String name, String host, int port) {
		logger = Logger.getLogger(name);
		this.name = name;
		this.host = host;
		this.port = port;
	}
	
	// abstract methods
	public abstract boolean start();
	public abstract void stop();
}
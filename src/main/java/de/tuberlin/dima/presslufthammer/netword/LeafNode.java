package de.tuberlin.dima.presslufthammer.netword;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

import de.tuberlin.dima.presslufthammer.ontology.Data;
import de.tuberlin.dima.presslufthammer.ontology.Query;

public class LeafNode extends Node {
	
	InetSocketAddress parentNode;
	
	// client part: send data (responses) 
	private ClientBootstrap clientBootstrap;
	
	// server part: listen for queries
	private ServerBootstrap serverBootstrap;
	
	public LeafNode(String name, int port) {
		super(name, port);
		
		ChannelFactory factory;
		
		// setup client
		factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		this.clientBootstrap = new ClientBootstrap(factory);
		
		serverBootstrap.bind();
	}
	
	public void setParentNode(String hostname, int port) {
		parentNode = new InetSocketAddress(hostname, port);
	}
	
	public InetSocketAddress getParentNode() {
		return parentNode;
	}
	
	@Override
	public Data answer(Query q) {
		// TODO replace with real method
		// TODO make private void
		return new Data(q.getId(), this.name);
	}
}
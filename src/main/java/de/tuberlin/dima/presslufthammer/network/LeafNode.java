package de.tuberlin.dima.presslufthammer.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.Channels;
import de.tuberlin.dima.presslufthammer.network.handler.ServerHandler;
import de.tuberlin.dima.presslufthammer.ontology.Result;
import de.tuberlin.dima.presslufthammer.ontology.Query;
import de.tuberlin.dima.presslufthammer.pressluft.Decoder;
import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;
import de.tuberlin.dima.presslufthammer.pressluft.Type;

public class LeafNode extends Node {
	
	InetSocketAddress parentNode;
	
	public LeafNode(String name, int port) {
		super(name, port);
		
		ChannelFactory factory;
		
		// setup server
		factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		this.serverBootstrap = new ServerBootstrap(factory);
		this.serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(new Decoder(), new ServerHandler(logger) {
					
					@Override
					public void handleResult(Result data, SocketAddress socketAddress) {
					}
					
					@Override
					public void handleQuery(Query query) {
						logger.debug("recieved query " + query.getId());
						sendAnswer(answer(query),logger,parentNode);
					}
				});
			}
		});
		
		this.serverBootstrap.bind(new InetSocketAddress(port));
	}
	
	private Result answer(Query q) {
		// TODO replace with real method
		// TODO make private void
		return new Result(q.getId(), this.name);
	}
	
	private static void sendAnswer(Result answer, Logger logger, InetSocketAddress parentNode) {
		// TODO replace with private void answer(Query q)
		logger.debug("sending " + answer.getId() + " to " + parentNode);
		
		Pressluft p;
		try {
			p = new Pressluft(Type.RESULT, Result.toByteArray(answer));
			sendPressLuft(p, parentNode, logger);
		} catch (IOException e) {
			System.err.println("FUCK");
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
	}
	
	// -- GETTERS AND SETTERS ---------------------------------------------------------------------
	
	public void setParentNode(String hostname, int port) {
		parentNode = new InetSocketAddress(hostname, port);
	}
	
	public InetSocketAddress getParentNode() {
		return parentNode;
	}
}
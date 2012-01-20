package de.tuberlin.dima.presslufthammer.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ServerChannelFactory;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.Channels;

import de.tuberlin.dima.presslufthammer.network.handler.ServerHandler;
import de.tuberlin.dima.presslufthammer.ontology.Result;
import de.tuberlin.dima.presslufthammer.ontology.Query;
import de.tuberlin.dima.presslufthammer.pressluft.old.Decoder;
import de.tuberlin.dima.presslufthammer.pressluft.old.Encoder;
import de.tuberlin.dima.presslufthammer.pressluft.old.Pressluft;
import de.tuberlin.dima.presslufthammer.pressluft.old.Type;

public final class LeafNode extends Node {
	
	private SocketAddress parentNode;
	private Channel parent;
	private ServerChannelFactory serverFactory;
	
	public LeafNode(final String name, String host, int port) {
		super(name, host, port);
	}
	
	private synchronized Result answer(Query q) {
		// TODO replace with real method
		// TODO make private void
		return new Result(q.getId(), this.name);
	}
	
	private synchronized void sendAnswer(Result answer) {
		// TODO replace with private void answer(Query q)
		logger.debug("sending " + answer.getId() + " to " + parentNode);
		
		Pressluft p;
		try {
			p = new Pressluft(Type.RESULT, Result.toByteArray(answer));
			if (parent != null) {
				parent.write(p);
			} else {
				logger.error("could not send message \"" + p + "\" to \"" + parentNode + "\"");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// -- GETTERS AND SETTERS ---------------------------------------------------------------------
	
	public synchronized void setParentNode(String hostname, int port) {
		parentNode = new InetSocketAddress(hostname, port);
	}
	
	public SocketAddress getParentNode() {
		return parentNode;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean start() {
		this.serverFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		this.channelGroup = new DefaultChannelGroup(this + "-allChannels");
		ChannelPipelineFactory pipelineFactory = new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("encoder", QEncoder.getInstance());
				pipeline.addLast("decoder", new QDecoder());
				pipeline.addLast("handler", new ServerHandler(name, channelGroup) {
					
					@Override
					public void handleResult(Result data, SocketAddress socketAddress) {
					}
					
					@Override
					public void handleQuery(Query query, Channel ch) {
						logger.debug("recieved query " + query.getId());
						sendAnswer(answer(query));
					}
					
					@Override
					public void handleConnection(Channel ch) {
						parent = ch;
					}
				});
				return pipeline;
			}
		};
		
		ServerBootstrap bootstrap = new ServerBootstrap(this.serverFactory);
		bootstrap.setOption("reuseAddress", true);
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);
		bootstrap.setPipelineFactory(pipelineFactory);
		
		Channel channel = bootstrap.bind(new InetSocketAddress(this.host, this.port));
		if (!channel.isBound()) {
			this.stop();
			return false;
		}
		
		return true;
	}
	
	@Override
	public void stop() {
		
		if (this.channelGroup != null) {
			this.channelGroup.close();
		}
		
		if (this.serverFactory != null) {
			this.serverFactory.releaseExternalResources();
		}
	}
}
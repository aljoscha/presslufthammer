package de.tuberlin.dima.presslufthammer.network;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import de.tuberlin.dima.presslufthammer.network.handler.ServerHandler;
import de.tuberlin.dima.presslufthammer.ontology.Result;
import de.tuberlin.dima.presslufthammer.ontology.Query;
import de.tuberlin.dima.presslufthammer.ontology.Task;
import de.tuberlin.dima.presslufthammer.pressluft.old.Decoder;
import de.tuberlin.dima.presslufthammer.pressluft.old.Encoder;

public final class RootNode extends ParentNode {
	
	private ChannelFactory clientFactory;
	
	public RootNode(String name, String host, int port) {
		super(name, host, port);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void handleQuery(Query q) {
		Task[] tasks = factorQuery(q);
		
		taskMap.put(q.getId(), tasks);
		
		for (Task task : tasks) {
			forwardTask(task);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean start() {
		this.clientFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		this.channelGroup = new DefaultChannelGroup(this + "-allChannels");
		ChannelPipelineFactory pipelineFactory = new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("encoder", Encoder.getInstance());
				pipeline.addLast("decoder", new Decoder());
				pipeline.addLast("handler", new ServerHandler(name, channelGroup) {
					
					@Override
					public void handleResult(Result data, SocketAddress socketAddress) {
						logger.debug("recieved result \"" + data.getId() + "\" from \"" + socketAddress + "\"");
					
						Task[] tasks = taskMap.get(data.getId());
						for (Task task : tasks) {
							if (((SocketAddress) task.getSolver()).equals(socketAddress)) {
								task.setSolution(data);
							}
						}

						if (isSolved(data.getId())) {
							Result res = mergeResults(extractResults(taskMap.get(data.getId())));
							logger.info("Answer to " + res.getId() + " : " + res.getValue());
							
							// TODO send back final result
						}
					}
					
					@Override
					public void handleQuery(Query query, Channel ch) {
					}
					
					@Override
					public void handleConnection(Channel ch) {
						connections.put(ch.getRemoteAddress(), ch);
					}
				});
				return pipeline;
			}
		};
		
		ClientBootstrap bootstrap = new ClientBootstrap(this.clientFactory);
		bootstrap.setOption("reuseAddress", true);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);
		bootstrap.setPipelineFactory(pipelineFactory);
		
		
		// connect to every child
		for(SocketAddress addr:childNodes){
			boolean connected = bootstrap.connect(addr).awaitUninterruptibly().isSuccess();
			if (!connected) {
				this.stop();
				return connected;
			}
		}
		
		return true;
	}
	
	@Override
	public void stop() {
		
		if (this.channelGroup != null) {
			this.channelGroup.close();
		}
		
		if (this.clientFactory != null) {
			this.clientFactory.releaseExternalResources();
		}
	}
}
package de.tuberlin.dima.presslufthammer.network;

import java.io.IOException;
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
import org.jboss.netty.channel.ServerChannelFactory;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import de.tuberlin.dima.presslufthammer.network.handler.ServerHandler;
import de.tuberlin.dima.presslufthammer.ontology.Result;
import de.tuberlin.dima.presslufthammer.ontology.Query;
import de.tuberlin.dima.presslufthammer.ontology.Task;
import de.tuberlin.dima.presslufthammer.pressluft.old.Decoder;
import de.tuberlin.dima.presslufthammer.pressluft.old.Encoder;
import de.tuberlin.dima.presslufthammer.pressluft.old.Pressluft;
import de.tuberlin.dima.presslufthammer.pressluft.old.Type;

public final class InnerNode extends ParentNode {
	
	SocketAddress parentNode;
	Channel parent;
	private ServerChannelFactory serverFactory;
	
	private ChannelFactory clientFactory;
	
	public InnerNode(String name, String host, int port) {
		super(name, host, port);
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
		this.channelGroup = new DefaultChannelGroup(this + "-allChannels");
		
		// Server (Leaf) part
		this.serverFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		ChannelPipelineFactory pipelineFactory = new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("encoder", Encoder.getInstance());
				pipeline.addLast("decoder", new Decoder());
				pipeline.addLast("handler", new ServerHandler(name, channelGroup) {
					
					@Override
					public void handleResult(Result data, SocketAddress socketAddress) {
					}
					
					@Override
					public void handleQuery(Query query, Channel ch) {
						logger.trace("recieved query " + query.getId());
						
						Task[] tasks = factorQuery(query);
						taskMap.put(query.getId(), tasks);
						for (Task task : tasks) {
							forwardTask(task);
						}
					}
					
					@Override
					public void handleConnection(Channel ch) {
						parent = ch;
					}
				});
				return pipeline;
			}
		};
		
		ServerBootstrap serverbootstrap = new ServerBootstrap(this.serverFactory);
		serverbootstrap.setOption("reuseAddress", true);
		serverbootstrap.setOption("child.tcpNoDelay", true);
		serverbootstrap.setOption("child.keepAlive", true);
		serverbootstrap.setPipelineFactory(pipelineFactory);
		
		Channel channel = serverbootstrap.bind(new InetSocketAddress(this.host, this.port));
		if (!channel.isBound()) {
			this.stop();
			return false;
		}
		
		// Client (Root) part
		this.clientFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		pipelineFactory = new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("encoder", Encoder.getInstance());
				pipeline.addLast("decoder", new Decoder());
				pipeline.addLast("handler", new ServerHandler(name, channelGroup) {
					
					@Override
					public void handleResult(Result data, SocketAddress socketAddress) {
						logger.trace("recieved result \"" + data.getId() + "\" from \"" + socketAddress + "\"");
						
						Task[] tasks = taskMap.get(data.getId());
						
						for (Task task : tasks) {
							if (((SocketAddress) task.getSolver()).equals(socketAddress)) {
								task.setSolution(data);
							}
						}
						
						if (isSolved(data.getId())) {
							Result res = mergeResults(extractResults(taskMap.get(data.getId())));
							logger.info("Answer to " + res.getId() + " : " + res.getValue());
							
							sendAnswer(res);
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
		
		ClientBootstrap clientbootstrap = new ClientBootstrap(this.clientFactory);
		clientbootstrap.setOption("reuseAddress", true);
		clientbootstrap.setOption("tcpNoDelay", true);
		clientbootstrap.setOption("keepAlive", true);
		clientbootstrap.setPipelineFactory(pipelineFactory);
		
		// connect to every child
		for (SocketAddress addr : childNodes) {
			boolean connected = clientbootstrap.connect(addr).awaitUninterruptibly().isSuccess();
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
		
		if (this.serverFactory != null) {
			this.serverFactory.releaseExternalResources();
		}
		
		if (this.clientFactory != null) {
			this.clientFactory.releaseExternalResources();
		}
	}
}
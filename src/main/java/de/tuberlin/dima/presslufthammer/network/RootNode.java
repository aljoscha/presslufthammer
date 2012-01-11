package de.tuberlin.dima.presslufthammer.network;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import de.tuberlin.dima.presslufthammer.network.handler.ServerHandler;
import de.tuberlin.dima.presslufthammer.ontology.Result;
import de.tuberlin.dima.presslufthammer.ontology.Query;
import de.tuberlin.dima.presslufthammer.ontology.Task;
import de.tuberlin.dima.presslufthammer.pressluft.Decoder;

public class RootNode extends ParentNode {
	
	public RootNode(String name, int port) {
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
						logger.debug("recieved data \"" + data.getId() + "\" from \"" + socketAddress + "\"");
						
//						synchronized(taskMap){
							Task[] tasks = taskMap.get(data.getId());
							
							for (Task task : tasks) {
								if (((SocketAddress) task.getSolver()).equals(socketAddress)) {
									task.setSolution(data);
								}
							}
							
							if (isSolved(data.getId())) {
								Result res = mergeResults(extractResults(taskMap.get(data.getId())));
								logger.info("Answer to " + res.getId() + " : " + res.getValue());
								
								// TODO send result to caller
							}
//						}
					}
					
					@Override
					public void handleQuery(Query query) {
					}
				});
			}
		});
		
		this.serverBootstrap.bind(new InetSocketAddress(port));
	}
	
	// --------------------------------------------------------------------------------------------
	
	public synchronized void handleQuery(Query q) {
		Task[] tasks = factorQuery(q);
		
		taskMap.put(q.getId(), tasks);
		
		for (Task task : tasks) {
			forwardTask(task);
		}
	}
}
package de.tuberlin.dima.presslufthammer.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import de.tuberlin.dima.presslufthammer.network.handler.ClientHandler;
import de.tuberlin.dima.presslufthammer.ontology.Result;
import de.tuberlin.dima.presslufthammer.ontology.Query;
import de.tuberlin.dima.presslufthammer.ontology.Task;
import de.tuberlin.dima.presslufthammer.pressluft.Encoder;
import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;
import de.tuberlin.dima.presslufthammer.pressluft.Type;

public class ParentNode extends Node {
	
	protected ChannelGroup childNodes;
	protected Map<Long, Task[]> taskMap;
	
	public ParentNode(String name, int port) {
		super(name, port);
		
		childNodes = new DefaultChannelGroup();
		taskMap = new HashMap<Long, Task[]>();
		
		ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		clientBootstrap = new ClientBootstrap(factory);
		clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(Encoder.getInstance(), new ClientHandler(logger));
			}
		});
	}
	
	protected void forwardTask(Task task) {
		logger.debug("sending " + task.getQuery().getId() + " to " + task.getSolver());
		Pressluft p;
		try {
			p = new Pressluft(Type.QUERY, Query.toByteArray(task.getQuery()));
			
			sendPressLuft(p, task.getSolver(), logger);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	// -- FACTORING AND MERGING -------------------------------------------------------------------
	
	protected Result mergeResults(Result[] results) {
		// TODO replace with real method
		// make static or move
		
		if (results.length <= 0) {
			return null;
		}
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(this.name + " [");
		for (Result data : results) {
			sb.append(data.getValue()).append(", ");
		}
		sb.replace(sb.length() - 2, sb.length(), "");
		sb.append("]");
		
		return new Result(results[0].getId(), sb.toString());
	}
	
	protected Task[] factorQuery(Query query) {
		// TODO replace with real method
		Task[] res = new Task[childNodes.size()];
		
		int i = 0;
		for (Channel ch : childNodes) {
			res[i++] = new Task(query, ch.getRemoteAddress());
		}
		
		return res;
	}
	
	// --------------------------------------------------------------------------------------------
	
	protected boolean isSolved(long id) {
		Task[] tmp = taskMap.get(id);
		
		for (Task task : tmp) {
			if (!task.isSolved()) {
				return false;
			}
		}
		
		return true;
	}
	
	protected static Result[] extractResults(Task[] tasks) {
		Result[] res = new Result[tasks.length];
		
		for (int i = 0; i < res.length; i++) {
			res[i] = tasks[i].getSolution();
		}
		
		return res;
	}
	
	// -- LIST METHODS ----------------------------------------------------------------------------
	
	public void addChildNode(String hostname, int port) {
		InetSocketAddress addr = new InetSocketAddress(hostname, port);
		ChannelFuture f = clientBootstrap.connect(addr);
		
		if (f.isSuccess()) {
			childNodes.add(f.getChannel());
		} else {
			logger.error("could not connet to " + addr);
		}
	}
	
//	public boolean revomeChildNode(String hostname, int port) {
//		return childNodes.remove(new InetSocketAddress(hostname, port));
//	}
}
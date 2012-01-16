package de.tuberlin.dima.presslufthammer.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.tuberlin.dima.presslufthammer.ontology.Result;
import de.tuberlin.dima.presslufthammer.ontology.Query;
import de.tuberlin.dima.presslufthammer.ontology.Task;
import de.tuberlin.dima.presslufthammer.pressluft.old.Pressluft;
import de.tuberlin.dima.presslufthammer.pressluft.old.Type;

public class ParentNode extends Node {
	
	protected Set<InetSocketAddress> childNodes;
	protected Map<Long, Task[]> taskMap;
	
	public ParentNode(String name, int port) {
		super(name, port);
		
		childNodes = new HashSet<InetSocketAddress>();
		taskMap = new HashMap<Long, Task[]>();
	}
	
	protected synchronized void forwardTask(Task task) {
		logger.debug("sending " + task.getQuery().getId() + " to " + task.getSolver());
		Pressluft p;
		try {
			p = new Pressluft(Type.QUERY, Query.toByteArray(task.getQuery()));
			
			sendPressLuft(p, task.getSolver());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// -- FACTORING AND MERGING -------------------------------------------------------------------
	
	protected synchronized Result mergeResults(Result[] results) {
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
	
	protected synchronized Task[] factorQuery(Query query) {
		// TODO replace with real method
		Task[] res = new Task[childNodes.size()];
		List<InetSocketAddress> tmp = new ArrayList<InetSocketAddress>(childNodes);
		
		for (int i = 0; i < res.length; i++) {
			res[i] = new Task(query, tmp.get(i));
		}
		
		return res;
	}
	
	// --------------------------------------------------------------------------------------------
	
	protected synchronized boolean isSolved(long id) {
		Task[] tmp = taskMap.get(id);
		
		for (Task task : tmp) {
			if (!task.isSolved()) {
				return false;
			}
		}
		
		return true;
	}
	
	protected static synchronized Result[] extractResults(Task[] tasks) {
		Result[] res = new Result[tasks.length];
		
		for (int i = 0; i < res.length; i++) {
			res[i] = tasks[i].getSolution();
		}
		
		return res;
	}
	
	// -- LIST METHODS ----------------------------------------------------------------------------
	
	public void addChildNode(String hostname, int port) {
		childNodes.add(new InetSocketAddress(hostname, port));
	}
	
	public boolean revomeChildNode(String hostname, int port) {
		return childNodes.remove(new InetSocketAddress(hostname, port));
	}
}
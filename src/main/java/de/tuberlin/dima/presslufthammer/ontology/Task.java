package de.tuberlin.dima.presslufthammer.ontology;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.jboss.netty.channel.Channel;

public class Task {
	
	// Query-ID
	private Query query;
	
	// answer to the query
	private Result solution;
	
	// the client, that solves the task
	private Channel solver;
	
	public Task(Query query, Channel ch) {
		this.query = query;
		this.solver = ch;
		this.solution = null;
	}
	
	public void setSolution(Result solution) {
		this.solution = solution;
	}
	
	public boolean isSolved() {
		return solution != null;
	}
	
	public Result getSolution() {
		return solution;
	}
	
	public Channel getSolversChannel() {
		return solver;
	}
	
	public Query getQuery() {
		return query;
	}
}
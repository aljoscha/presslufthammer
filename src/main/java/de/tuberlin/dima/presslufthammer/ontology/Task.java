package de.tuberlin.dima.presslufthammer.ontology;

import java.net.InetSocketAddress;

public class Task {
	
	// Query-ID
	private Query query;
	
	// answer to the query
	private Result solution;
	
	// the client, that solves the task
	private InetSocketAddress solver;
	
	public Task(Query query, InetSocketAddress socketAddress) {
		this.query = query;
		this.solver = socketAddress;
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
	
	public InetSocketAddress getSolver() {
		return solver;
	}
	
	public Query getQuery() {
		return query;
	}
}
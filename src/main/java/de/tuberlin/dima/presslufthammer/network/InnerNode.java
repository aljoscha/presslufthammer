package de.tuberlin.dima.presslufthammer.network;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import de.tuberlin.dima.presslufthammer.ontology.Data;
import de.tuberlin.dima.presslufthammer.ontology.Query;

public class InnerNode extends Node {
	
	Set<InetSocketAddress> childNodes;
	InetSocketAddress parentNode;
	
	public InnerNode(String name, int port) {
		super(name, port);
		childNodes = new HashSet<InetSocketAddress>();
	}
	
	public void addChildNode(String hostname, int port) {
		childNodes.add(new InetSocketAddress(hostname, port));
	}
	
	public boolean revomeChildNode(String hostname, int port) {
		return childNodes.remove(new InetSocketAddress(hostname, port));
	}
	
	public void setParentNode(String hostname, int port) {
		parentNode = new InetSocketAddress(hostname, port);
	}
	
	public InetSocketAddress getParentNode() {
		return parentNode;
	}

	@Override
	public Data answer(Query q) {
		// TODO Auto-generated method stub
		return null;
	}
}
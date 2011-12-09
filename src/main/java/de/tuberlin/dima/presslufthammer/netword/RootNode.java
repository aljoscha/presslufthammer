package de.tuberlin.dima.presslufthammer.netword;

import java.net.InetSocketAddress;
import java.util.Set;

public class RootNode extends Node {
	
	public RootNode(String name, int port) {
		super(name, port);
	}
	
	Set<InetSocketAddress> childNodes;
	
	public void addData(Object data) {
		// TODO
		// replace Object with real type
		// adds data to the network
	}
	
	public void addNode(Node node) {
		// TODO
		// adds a node to the network
		// the network decides what kind of node it will be
		// may not be implemented
	}
	
	public void addNode(InnerNode node) {
		// TODO
	}
	
	public void addNode(LeafNode node) {
		// TODO
		// adds a leafnode to the network
	}
	
	public void addChildNode(String hostname, int port) {
		childNodes.add(new InetSocketAddress(hostname, port));
	}
	
	public boolean revomeChildNode(String hostname, int port) {
		return childNodes.remove(new InetSocketAddress(hostname, port));
	}
}
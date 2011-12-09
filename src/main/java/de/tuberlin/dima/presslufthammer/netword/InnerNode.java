package de.tuberlin.dima.presslufthammer.netword;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

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
}
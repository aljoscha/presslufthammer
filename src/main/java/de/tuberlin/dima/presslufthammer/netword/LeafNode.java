package de.tuberlin.dima.presslufthammer.netword;

import java.net.InetSocketAddress;

public class LeafNode extends Node {
	
	InetSocketAddress parentNode;
	
	public LeafNode(String name, int port) {
		super(name, port);
	}
	
	public void setParentNode(String hostname, int port) {
		parentNode = new InetSocketAddress(hostname, port);
	}
	
	public InetSocketAddress getParentNode() {
		return parentNode;
	}
}
package de.tuberlin.dima.presslufthammer.network;

import org.jboss.netty.channel.SimpleChannelHandler;

public abstract class ClientHandler<T> extends SimpleChannelHandler {
	
	abstract void sendMessage(T message);
}
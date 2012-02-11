package de.tuberlin.dima.presslufthammer.transport.util;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;

import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;

/**
 * @author feichh
 * 
 */
public class ServingChannel {
	private Channel channel;
	private SocketAddress servingAddress;

	public static byte[] intToByte(int integer) {
		ChannelBuffer buffer = ChannelBuffers.buffer(4);
		buffer.writeInt(integer);
		return buffer.array();
	}

	public static int byteToInt(byte[] bytes) {
		ChannelBuffer buffer = ChannelBuffers.buffer(4);
		buffer.writeBytes(bytes);
		int integer = buffer.readInt();
		return integer;
	}

	// public ServingChannel(Channel channel, SocketAddress servingAddress) {
	//
	// this.setChannel(channel);
	// this.setServingAddress(servingAddress);
	// }

	public ServingChannel(Channel channel, byte[] portbytes) {

		this.setChannel(channel);
		// int port = Integer.parseInt(new String(portbytes));
		int port = byteToInt(portbytes);
		String host = channel.getRemoteAddress().toString().split(":")[0];
		SocketAddress servingAddress = new InetSocketAddress(host, port);
		this.servingAddress = servingAddress;
	}

	public SocketAddress getServingAddress() {
		return servingAddress;
	}

	public void setServingAddress(SocketAddress servingAddress) {
		this.servingAddress = servingAddress;
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	public void write(SimpleMessage message) {
		channel.write(message);
	}

	public Object getRemoteAddress() {
		return channel.getRemoteAddress();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj instanceof Channel) {
			return channel.equals(obj);
		} else {
			return super.equals(obj);
		}
	}
}
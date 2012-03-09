package de.tuberlin.dima.presslufthammer.transport.util;

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
	private int servingPort;

	/**
	 * Converts an integer into a byte[] using a ChannelBuffer.
	 * 
	 * @param integer
	 * @return byte representation of the integer
	 */
	public static byte[] intToByte(int integer) {
		ChannelBuffer buffer = ChannelBuffers.buffer(4);
		buffer.writeInt(integer);
		return buffer.array();
	}

	/**
	 * Converts the byte[] to an integer using a ChannelBuffer.
	 * 
	 * @param bytes
	 * @return integer value of the bytes
	 */
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

	/**
	 * @param channel
	 *            the channel through which the node is connected
	 * @param portbytes
	 *            port in the form of a byte[]
	 */
	public ServingChannel(Channel channel, byte[] portbytes) {

		this.setChannel(channel);
		// int port = Integer.parseInt(new String(portbytes));
		int port = byteToInt(portbytes);
		this.setServingPort(port);
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

	public int getServingPort() {
		return servingPort;
	}

	public void setServingPort(int servingPort) {
		this.servingPort = servingPort;
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	/**
	 * Wraps channel.write().
	 * 
	 * @param message
	 */
	public void write(SimpleMessage message) {
		channel.write(message);
	}
	
	public void write(Object obj) {
		channel.write(obj);
	}

	/**
	 * @return channel.getRemoteAddress()
	 */
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return channel + " " + servingAddress;
	}

}
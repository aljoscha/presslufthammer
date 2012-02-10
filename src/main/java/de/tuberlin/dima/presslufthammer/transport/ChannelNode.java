/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.Type;

/**
 * @author feichh
 * 
 */
public abstract class ChannelNode implements Closeable {

	/**
	 * All connected/open netty channels;
	 */
	ChannelGroup openChannels = new DefaultChannelGroup();

	/**
	 * see connectNReg( SocketAddress)
	 * 
	 * @param host
	 *            IP or hostname of target
	 * @param port
	 *            host's port
	 * @return true if connection was established successfully
	 */
	public boolean connectNReg(String host, int port) {
		return connectNReg(new InetSocketAddress(host, port));
	}

	/**
	 * @param address
	 *            target address
	 * @return true if connection was established successfully
	 */
	public boolean connectNReg(SocketAddress address) {
		return false;
	}

	/**
	 * @param query
	 */
	public void query(String query) {
		if (query != null && query.length() > 0) {
			// Query q = new Query(query);
			query(new SimpleMessage(Type.CLIENT_QUERY, (byte) -1,
					query.getBytes()));
		}
		return;
	}

	/**
	 * @param query
	 */
	public void query(SimpleMessage query) {
		return;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Closeable#close()
	 */
	public void close() throws IOException {
		openChannels.close().awaitUninterruptibly();
	}

	public abstract void messageReceived(ChannelHandlerContext ctx,
			MessageEvent e);

	public void removeChannel(Channel channel) {
		openChannels.remove(channel);
	}
}

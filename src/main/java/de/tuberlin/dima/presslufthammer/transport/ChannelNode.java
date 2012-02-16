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

import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.Type;

/**
 * Abstract superclass for Netty based network nodes.
 * 
 * @author feichh
 * 
 */
public abstract class ChannelNode implements Closeable {

	/**
	 * All connected/open netty channels;
	 */
	public ChannelGroup openChannels = new DefaultChannelGroup();

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
	 * Attempts connecting to the supplied target address.
	 * 
	 * @param address
	 *            target address
	 * @return true if connection was established successfully
	 */
	public boolean connectNReg(SocketAddress address) {
		return false;
	}

	/**
	 * Handles the supplied query string.
	 * 
	 * @param query
	 *            String representation of a {@link Query}
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
	 * Handles a {@link SimpleMessage} containing a {@link Query}.
	 * 
	 * @param query
	 *            {@link Query} wrapped in a {@link SimpleMessage}
	 */
	public void query(SimpleMessage query) {
		return;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Closeable#close()
	 */
	public void close() {
		openChannels.disconnect().awaitUninterruptibly();
		openChannels.close().awaitUninterruptibly();
	}

	/**
	 * Method that received messages from the ChannelHandler.
	 * 
	 * @param ctx
	 *            {@link ChannelHandlerContext}
	 * @param e
	 *            {@link MessageEvent}
	 */
	public abstract void messageReceived(ChannelHandlerContext ctx,
			MessageEvent e);

	/**
	 * Removes a Channel from this.openChannels.
	 * 
	 * @param channel
	 */
	public void removeChannel(Channel channel) {
		openChannels.remove(channel);
	}
}

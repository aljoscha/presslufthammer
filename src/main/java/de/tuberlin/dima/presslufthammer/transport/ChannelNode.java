/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;

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
	 * @param port
	 * @return true if connection was established successfully
	 */
	public boolean connectNReg(String host, int port) {
		return connectNReg(new InetSocketAddress(host, port));
	}

	/**
	 * @param address
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
			query(Pressluft.createQueryMSG(query));
		}
	}

	/**
	 * @param query
	 */
	public void query(Pressluft query) {
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
}

/**
 * 
 */
package de.tuberlin.dima.presslufthammer.testing;

import java.net.InetSocketAddress;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;

/**
 * @author feichh
 * 
 */
public class ClientHandler extends SimpleChannelHandler {
	private final Logger log = LoggerFactory.getLogger(getClass());
	private final ChannelGroup openChannels;
	private final CLIClient client;

	/**
	 * @param client
	 * @param channelGroup
	 */
	public ClientHandler(CLIClient client, ChannelGroup channelGroup) {
		this.client = client;
		this.openChannels = channelGroup;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.jboss.netty.channel.SimpleChannelHandler#exceptionCaught(org.jboss.
	 * netty.channel.ChannelHandlerContext,
	 * org.jboss.netty.channel.ExceptionEvent)
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		// TODO
		Throwable cause = e.getCause();
		log.error("caught an exception", cause);
		ctx.getChannel().close();
		// super.exceptionCaught( ctx, e);
		ctx.sendUpstream(e);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.jboss.netty.channel.SimpleChannelHandler#channelConnected(org.jboss
	 * .netty.channel.ChannelHandlerContext,
	 * org.jboss.netty.channel.ChannelStateEvent)
	 */
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		// TODO difference between channelConnected / channelOpen ???
		this.openChannels.add(e.getChannel());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.jboss.netty.channel.SimpleChannelHandler#messageReceived(org.jboss
	 * .netty.channel.ChannelHandlerContext,
	 * org.jboss.netty.channel.MessageEvent)
	 */
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		if (e.getMessage() instanceof Pressluft) {
			Pressluft prsslft = (Pressluft) e.getMessage();
			log.debug("received: " + prsslft.getType() + " from "
					+ e.getRemoteAddress());
			switch (prsslft.getType()) {
			case ACK:
				break;
			case INFO:
				// InetSocketAddress innerAddress =
				// getSockAddrFromBytes(prsslft.getPayload());
				// client.connectNReg( innerAddress);
				break;
			case QUERY:
			case REGINNER:
			case REGLEAF:
			case RESULT:
				client.handleResult(prsslft);
			case UNKNOWN:
				break;

			}
			// e.getChannel().write(e.getMessage());
		} else {
			super.messageReceived(ctx, e);
		}
	}

	/**
	 * @param payload
	 * @return
	 */
	private InetSocketAddress getSockAddrFromBytes(byte[] payload) {
		// TODO
		String temp = new String(payload);
		log.debug(temp);
		String[] split = temp.split(":");
		String ipaddr = split[0].replaceAll("/", "");
		int port = Integer.parseInt(split[1]) + 1;
		log.debug(ipaddr + " " + port);
		return new InetSocketAddress(ipaddr, port);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.jboss.netty.channel.SimpleChannelHandler#handleDownstream(org.jboss
	 * .netty.channel.ChannelHandlerContext,
	 * org.jboss.netty.channel.ChannelEvent)
	 */
	@Override
	public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception {
		// Sending the event downstream (outbound)
		ctx.sendDownstream(e);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.jboss.netty.channel.SimpleChannelHandler#handleUpstream(org.jboss
	 * .netty.channel.ChannelHandlerContext,
	 * org.jboss.netty.channel.ChannelEvent)
	 */
	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception {

		// Log all channel state changes.
		if (e instanceof ChannelStateEvent) {
			log.info("Channel state changed: " + e);
		}

		super.handleUpstream(ctx, e);
	}
}

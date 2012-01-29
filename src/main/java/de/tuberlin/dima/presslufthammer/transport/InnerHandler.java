/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;

/**
 * @author feichh
 * 
 * @deprecated replaced by generic version
 */
public class InnerHandler extends SimpleChannelHandler {
	private final Logger log = LoggerFactory.getLogger(getClass());
	private final ChannelGroup openChannels;
	private final Inner inner;

	/**
	 * @param inner
	 */
	public InnerHandler(Inner inner) {
		this.inner = inner;
		this.openChannels = inner.openChannels;

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
		if (e.getMessage() instanceof SimpleMessage) {
			SimpleMessage message = (SimpleMessage) e.getMessage();
			log.info(message.getType() + " from "
					+ e.getRemoteAddress().toString());

			switch (message.getType()) {
			case ACK:
				// not used yet
				break;
			case INFO:
				// not used yet
				break;
			case INTERNAL_QUERY:
				// TODO split query and hand parts over to children
				inner.query( message);
				break;
			case REGINNER:
				// not used yet
				break;
			case REGLEAF:
				// TODO handle new leaf connection
				openChannels.add(e.getChannel());
				inner.regChild(e.getChannel());
				break;
			case INTERNAL_RESULT:
				// TODO accumulate results; combine them; send them to parent;
				inner.handleResult(message);
				break;
			case UNKNOWN:
				// not used yet
				break;

			}
		} else {
			super.messageReceived(ctx, e);
		}
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

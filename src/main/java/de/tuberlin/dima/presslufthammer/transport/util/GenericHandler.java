package de.tuberlin.dima.presslufthammer.transport.util;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.transport.ChannelNode;
import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;

/**
 * @author feichh
 * @author Aljoscha Krettek
 * 
 */
public class GenericHandler extends SimpleChannelHandler {
	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ChannelNode node;
	private final ChannelGroup openChannels;

	public GenericHandler(ChannelNode node) {
		this.node = node;
		openChannels = node.openChannels;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		// TODO
		Throwable cause = e.getCause();
		log.error("Caught an exception: {}", cause);

		node.removeChannel(ctx.getChannel());
		// super.exceptionCaught( ctx, e);
		ctx.sendUpstream(e);
	}

	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		log.debug("Channel opened: " + e.getChannel());
		openChannels.add(e.getChannel());
		super.channelOpen(ctx, e);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		if (e.getMessage() instanceof SimpleMessage) {
			node.messageReceived(ctx, e);
		} else {
			super.messageReceived(ctx, e);
		}
	}
//
//	@Override
//	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
//			throws Exception {
//		
//		node.removeChannel(ctx.getChannel());
//		super.channelClosed(ctx, e);
//	}
//
}

package de.tuberlin.dima.presslufthammer.transport;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;

/**
 * @author feichh
 * @author Aljoscha Krettek
 * 
 */
public class ClientHandler extends SimpleChannelHandler {
	private final Logger log = LoggerFactory.getLogger(getClass());
	private final CLIClient client;

	/**
	 * @param client
	 * @param channelGroup
	 */
	public ClientHandler(CLIClient client) {
		this.client = client;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		Throwable cause = e.getCause();
		log.error("Caught exception: {}", cause);
		ctx.getChannel().close();
		// super.exceptionCaught( ctx, e);
		ctx.sendUpstream(e);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		log.debug("Message received from {}.", e.getRemoteAddress());
		if (e.getMessage() instanceof SimpleMessage) {
			SimpleMessage message = ((SimpleMessage) e.getMessage());
			log.debug("Message: {}", message.toString());
			switch (message.getType()) {
			case ACK:
			case INFO:
			case INTERNAL_QUERY:
			case REGINNER:
			case REGLEAF:
				break;
			case CLIENT_RESULT:
				client.handleResult(message);
			case UNKNOWN:
				break;
			}
		} else {
			super.messageReceived(ctx, e);
		}
	}

	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception {

		// Log all channel state changes.
		if (e instanceof ChannelStateEvent) {
			log.debug("Channel state changed: {}", e);
		}

		super.handleUpstream(ctx, e);
	}
}

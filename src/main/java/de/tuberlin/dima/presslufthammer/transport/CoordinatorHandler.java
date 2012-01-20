package de.tuberlin.dima.presslufthammer.transport;

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
 * @author Aljoscha Krettek
 * 
 */
public class CoordinatorHandler extends SimpleChannelHandler {
	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Coordinator coordinator;
	private final ChannelGroup openChannels;

	public CoordinatorHandler(Coordinator coordinator) {
		this.coordinator = coordinator;
		openChannels = coordinator.openChannels;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		// TODO
		Throwable cause = e.getCause();
		log.error("Caught an exception: {}", cause);

		coordinator.removeChannel(ctx.getChannel());
		// super.exceptionCaught( ctx, e);
		ctx.sendUpstream(e);
	}
	
	public ChannelGroup getOpenChannels() {
	    return openChannels;
	}

	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		log.debug("Channel opened: " + e.getChannel().getRemoteAddress());
		openChannels.add(e.getChannel());
		super.channelOpen(ctx, e);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		log.debug("Message received from {}.", e.getRemoteAddress());
		if (e.getMessage() instanceof SimpleMessage) {
			SimpleMessage pressluft = ((SimpleMessage) e.getMessage());
			log.debug("Message: {}", pressluft.toString());

			switch (pressluft.getType()) {
			case ACK:
				break;
			case REGINNER:
				coordinator.addInner(e.getChannel());
				break;
			case REGLEAF:
				coordinator.addLeaf(e.getChannel());
				break;
			case RESULT:
			    // Send the result to the coordinator so it can be assembled
				coordinator.handleResult(pressluft);
				break;
			case QUERY:
				// TODO get the query to the root node
				coordinator.query(pressluft, e.getChannel());
				break;
			case UNKNOWN:
				break;
			case INFO:
				break;
			case REGCLIENT:
				coordinator.addClient(e.getChannel());
				break;
			}

			e.getChannel()
					.write(new SimpleMessage(
							de.tuberlin.dima.presslufthammer.transport.messages.Type.ACK,
							(byte) 0, new byte[] { (byte) 0 }));
		} else {
			super.messageReceived(ctx, e);
		}
	}

}

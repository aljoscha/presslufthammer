/**
 * 
 */
package de.tuberlin.dima.presslufthammer.query;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * based on / taken from: https://github.com/brunodecarvalho/netty-tutorials
 * 
 * OneToOneEncoder implementation that converts an Envelope instance into a
 * ChannelBuffer.
 * 
 * Since the encoder is stateless, a single instance can be shared among all
 * pipelines, hence the @Sharable annotation and the singleton instantiation.
 */
@ChannelHandler.Sharable
public class QEncoder extends OneToOneEncoder {

	// constructors
	// ---------------------------------------------------------------------------------------------------

	private QEncoder() {
	}

	// public static methods
	// ------------------------------------------------------------------------------------------

	public static QEncoder getInstance() {
		return InstanceHolder.INSTANCE;
	}

	public static ChannelBuffer encodeMessage(Query message)
			throws IllegalArgumentException {
		// you can move these verifications "upper" (before writing to the
		// channel) in order not to cause a
		// channel shutdown

		if (message.getId() < 0) {
			throw new IllegalArgumentException("Invalid query ID");
		}

		// qid(1b) + select length(4b) + select(nb)
		int size = 5 + message.getFrom().getBytes().length;

		byte[][] select = new byte[message.getSelect().length][];
		for (int i = 0; i < message.getSelect().length; i++) {
			select[i] = message.getSelect()[i].getBytes();
		}

		ChannelBuffer buffer = ChannelBuffers.buffer(size);
		// buffer.writeByte(message.getType().getByteValue());
		// buffer.writeByte(message.getQueryID());
		// buffer.writeInt(message.getPayload().length);
		// buffer.writeBytes(message.getPayload());

		return buffer;
	}

	// OneToOneEncoder
	// ------------------------------------------------------------------------------------------------

	@Override
	protected Object encode(ChannelHandlerContext channelHandlerContext,
			Channel channel, Object msg) throws Exception {
		if (msg instanceof Query) {
			return encodeMessage((Query) msg);
		} else {
			return msg;
		}
	}

	// private classes
	// ------------------------------------------------------------------------------------------------

	private static final class InstanceHolder {
		private static final QEncoder INSTANCE = new QEncoder();
	}
}
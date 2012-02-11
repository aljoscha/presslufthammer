/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport.messages;

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
public class Encoder extends OneToOneEncoder {

	// constructors
	// ---------------------------------------------------------------------------------------------------

	private Encoder() {
	}

	// public static methods
	// ------------------------------------------------------------------------------------------

	public static Encoder getInstance() {
		return InstanceHolder.INSTANCE;
	}

	public static ChannelBuffer encodeMessage(SimpleMessage message)
			throws IllegalArgumentException {
		// you can move these verifications "upper" (before writing to the
		// channel) in order not to cause a
		// channel shutdown

		if ((message.getType() == null) || (message.getType() == Type.UNKNOWN)) {
			throw new IllegalArgumentException(
					"Message type cannot be null or UNKNOWN");
		}

		int payloadLen = (message.getPayload() != null)? message.getPayload().length: 0;
		if ((message.getPayload() == null)
				|| (message.getPayload().length == 0)) {
			throw new IllegalArgumentException(
					"Message payload cannot be null or empty");
		}

		// type(1b) + qid(1b) + payload length(4b) + payload(nb)
		int size = 6 + payloadLen;

		ChannelBuffer buffer = ChannelBuffers.buffer(size);
		buffer.writeByte(message.getType().getByteValue());
		buffer.writeByte(message.getQueryID());
		buffer.writeInt(payloadLen);
		buffer.writeBytes(message.getPayload());

		return buffer;
	}

	// OneToOneEncoder
	// ------------------------------------------------------------------------------------------------

	@Override
	protected Object encode(ChannelHandlerContext channelHandlerContext,
			Channel channel, Object msg) throws Exception {
		if (msg instanceof SimpleMessage) {
			return encodeMessage((SimpleMessage) msg);
		} else {
			return msg;
		}
	}

	// private classes
	// ------------------------------------------------------------------------------------------------

	private static final class InstanceHolder {
		private static final Encoder INSTANCE = new Encoder();
	}
}
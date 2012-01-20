/**
 * 
 */
package de.tuberlin.dima.presslufthammer.query;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;

/**
 * based on https://github.com/brunodecarvalho/netty-tutorials
 */
public class QDecoder extends ReplayingDecoder<QDecoder.DecodingState> {

	// internal vars

	private Query message;
	private byte[] bytes;

	// constructors

	public QDecoder() {
		this.reset();
	}

	// ReplayingDecoder

	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer, DecodingState state) throws Exception {

		// notice the switch fall-through
		switch (state) {
		case LEN:
			int len = buffer.readInt();
			if (len <= 0) {
				throw new Exception();
			}
			bytes = new byte[len];
			checkpoint(DecodingState.QUERY);
		case QUERY:
			buffer.readBytes(bytes);
			message = new Query(bytes);
			return message;
		default:
			throw new Exception("Unknown decoding state: " + state);
		}
	}

	// private helpers

	private void reset() {
		checkpoint(DecodingState.LEN);
		this.message = new Query();
	}

	// private classes

	public enum DecodingState {
		// constants
		LEN, QUERY
	}
}
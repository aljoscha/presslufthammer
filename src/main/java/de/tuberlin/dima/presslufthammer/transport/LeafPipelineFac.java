/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;

import de.tuberlin.dima.presslufthammer.query.QDecoder;
import de.tuberlin.dima.presslufthammer.query.QEncoder;
import de.tuberlin.dima.presslufthammer.transport.messages.Decoder;
import de.tuberlin.dima.presslufthammer.transport.messages.Encoder;

/**
 * @author feichh
 * 
 */
public class LeafPipelineFac implements ChannelPipelineFactory {

	private final Leaf leaf;

	/**
	 * @param leaf
	 */
	public LeafPipelineFac(Leaf leaf) {
		this.leaf = leaf;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jboss.netty.channel.ChannelPipelineFactory#getPipeline()
	 */
	public ChannelPipeline getPipeline() throws Exception {
		// TODO add required ChannelHandlers
		ChannelPipeline pipe = pipeline();

		pipe.addLast("Encoder", Encoder.getInstance());
		pipe.addLast("Decoder", new Decoder());
		pipe.addLast("QueryEncoder", QEncoder.getInstance());
		pipe.addLast("QueryDecoder", new QDecoder());
		pipe.addLast("LeafHandler", new LeafHandler(leaf));
		return pipe;
	}

}

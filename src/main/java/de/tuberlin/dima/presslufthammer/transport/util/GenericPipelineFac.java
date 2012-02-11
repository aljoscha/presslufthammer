/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport.util;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;

import de.tuberlin.dima.presslufthammer.transport.ChannelNode;
import de.tuberlin.dima.presslufthammer.transport.messages.Decoder;
import de.tuberlin.dima.presslufthammer.transport.messages.Encoder;

/**
 * @author feichh
 * 
 */
public class GenericPipelineFac implements ChannelPipelineFactory {

	private GenericHandler handler;

	public GenericPipelineFac(GenericHandler hand) {
		this.handler = hand;
	}
	
	public GenericPipelineFac(ChannelNode node) {
		this.handler = new GenericHandler(node);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jboss.netty.channel.ChannelPipelineFactory#getPipeline()
	 */
	public ChannelPipeline getPipeline() throws Exception {
		// TODO

		ChannelPipeline pipe = pipeline();
		pipe.addLast("Encoder", Encoder.getInstance());
		pipe.addLast("Decoder", new Decoder());
//		pipe.addLast("QueryEncoder", QEncoder.getInstance());
//		pipe.addLast("QueryDecoder", new QDecoder());
		pipe.addLast("Handler", handler);

		return pipe;
	}

}

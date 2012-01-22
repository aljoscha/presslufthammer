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
public class CoordinatorPipelineFac implements ChannelPipelineFactory {

	private CoordinatorHandler handler;

	public CoordinatorPipelineFac(CoordinatorHandler hand) {
		this.handler = hand;
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
		pipe.addLast("QueryEncoder", QEncoder.getInstance());
		pipe.addLast("QueryDecoder", new QDecoder());
		pipe.addLast("CoordinatorHandler", handler);

		return pipe;
	}

}

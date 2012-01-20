/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;

import de.tuberlin.dima.presslufthammer.transport.messages.Decoder;
import de.tuberlin.dima.presslufthammer.transport.messages.Encoder;

/**
 * @author feichh
 * 
 */
public class InnerPipelineFac implements ChannelPipelineFactory {
	private final Inner inner;

	/**
	 * @param channelGroup
	 */
	public InnerPipelineFac(Inner inner) {
		this.inner = inner;
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
		pipe.addLast("InnerHandler", new InnerHandler(inner));
		return pipe;
	}

}

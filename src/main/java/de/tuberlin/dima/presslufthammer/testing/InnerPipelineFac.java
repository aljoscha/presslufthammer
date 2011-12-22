/**
 * 
 */
package de.tuberlin.dima.presslufthammer.testing;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;

import de.tuberlin.dima.presslufthammer.pressluft.Decoder;
import de.tuberlin.dima.presslufthammer.pressluft.Encoder;

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

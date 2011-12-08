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
public class InnerPipelineFac implements ChannelPipelineFactory
{
	private ChannelGroup channelGroup;
	
	/**
	 * @param channelGroup
	 */
	public InnerPipelineFac( ChannelGroup channelGroup)
	{
		super();
		this.channelGroup = channelGroup;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.ChannelPipelineFactory#getPipeline()
	 */
	@Override
	public ChannelPipeline getPipeline() throws Exception
	{
		// TODO Auto-generated method stub
		ChannelPipeline pipe = pipeline();
		pipe.addLast( "Encoder", Encoder.getInstance());
		pipe.addLast( "Decoder", new Decoder());
		pipe.addLast( "InnerHandler", new InnerHandler( channelGroup));
		return pipe;
	}

}

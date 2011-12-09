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
public class CoordinatorPipelineFac implements ChannelPipelineFactory
{

	private ChannelGroup openChannels;
	private CoordinatorHandler handler;
	
	public CoordinatorPipelineFac( ChannelGroup cGroup, CoordinatorHandler hand)
	{
		assert( cGroup != null);
		this.openChannels = cGroup;
		this.handler = hand;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jboss.netty.channel.ChannelPipelineFactory#getPipeline()
	 */
	@Override
	public ChannelPipeline getPipeline() throws Exception
	{
		// TODO
		
		ChannelPipeline pipe = pipeline();
		pipe.addLast( "Encoder", Encoder.getInstance());
		pipe.addLast( "Decoder", new Decoder());
		pipe.addLast( "CoordinatorHandler", handler);
		
		return pipe;
	}

}

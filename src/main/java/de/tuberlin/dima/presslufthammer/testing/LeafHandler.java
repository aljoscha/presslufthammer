/**
 * 
 */
package de.tuberlin.dima.presslufthammer.testing;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;

/**
 * @author feichh
 * 
 */
public class LeafHandler extends SimpleChannelHandler
{
	private final Logger	log	= LoggerFactory.getLogger( getClass());
	private ChannelGroup	openChannels;

	public LeafHandler( ChannelGroup channelGroup)
	{
		this.openChannels = channelGroup;
	}

	@Override
	public void channelConnected( ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception
	{
		this.openChannels.add( e.getChannel());
	}

	@Override
	public void messageReceived( ChannelHandlerContext ctx, MessageEvent e)
			throws Exception
	{
		if( e.getMessage() instanceof Pressluft)
		{
			log.debug( "received: " + ((Pressluft) e.getMessage()).getType());
			// e.getChannel().write(e.getMessage());
		}
		else
		{
			super.messageReceived( ctx, e);
		}
	}

	// Sending the event downstream (outbound)
	@Override
	public void handleDownstream( ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception
	{
		ctx.sendDownstream( e);
	}

	@Override
	public void handleUpstream( ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception
	{

		// Log all channel state changes.
		if( e instanceof ChannelStateEvent)
		{
			log.info( "Channel state changed: " + e);
		}

		super.handleUpstream( ctx, e);
	}
}

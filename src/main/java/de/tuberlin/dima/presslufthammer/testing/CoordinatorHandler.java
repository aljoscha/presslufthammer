/**
 * 
 */
package de.tuberlin.dima.presslufthammer.testing;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;

/**
 * @author feichh
 * 
 */
public class CoordinatorHandler extends SimpleChannelHandler
{
	/**
	 * Logger
	 */
	private final Logger				logger	= Logger.getLogger( getClass());

	private final ChannelGroup	openChannels;															// = new
																																				// DefaultChannelGroup(
																																				// "");

	/**
	 * @param coord
	 */
	public CoordinatorHandler( ChannelGroup channels)
	{
		openChannels = channels;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.jboss.netty.channel.SimpleChannelHandler#channelOpen(org.jboss.netty
	 * .channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
	 */
	@Override
	public void channelOpen( ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception
	{
		logger.debug( "channelOpen " + e.getChannel().getRemoteAddress());
		openChannels.add( e.getChannel());
		super.channelOpen( ctx, e);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.jboss.netty.channel.SimpleChannelUpstreamHandler#messageReceived(org
	 * .jboss.netty.channel.ChannelHandlerContext,
	 * org.jboss.netty.channel.MessageEvent)
	 */
	@Override
	public void messageReceived( ChannelHandlerContext ctx, MessageEvent e)
			throws Exception
	{
		logger.debug( "messageReceived " + e.getRemoteAddress());
		if( e.getMessage() instanceof Pressluft)
		{
			logger.debug( ((Pressluft) e.getMessage()));
			e.getChannel().write( new Pressluft( de.tuberlin.dima.presslufthammer.pressluft.Type.ACK, new byte[] { (byte) 0}));
		} else {
			super.messageReceived( ctx, e);
		}
	}

}

/**
 * 
 */
package de.tuberlin.dima.presslufthammer.testing;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
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
public class CoordinatorHandler extends SimpleChannelHandler
{
	/**
	 * Logger
	 */
	private final Logger				log	= LoggerFactory.getLogger( getClass());

	private final Coordinator		coord;
	private final ChannelGroup	openChannels;

	/**
	 * @param coord
	 */
	public CoordinatorHandler( Coordinator coord)
	{
		this.coord = coord;
		openChannels = coord.openChannels;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.jboss.netty.channel.SimpleChannelHandler#exceptionCaught(org.jboss.
	 * netty.channel.ChannelHandlerContext,
	 * org.jboss.netty.channel.ExceptionEvent)
	 */
	@Override
	public void exceptionCaught( ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception
	{
		// TODO
		Throwable cause = e.getCause();
		log.error( "caught an exception", cause);
		
		coord.removeChannel( ctx.getChannel());
		// super.exceptionCaught( ctx, e);
    ctx.sendUpstream(e);
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
		log.debug( "channelOpen " + e.getChannel().getRemoteAddress());
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
		log.debug( "messageReceived " + e.getRemoteAddress());
		if( e.getMessage() instanceof Pressluft)
		{
			Pressluft prsslft = ((Pressluft) e.getMessage());
			log.debug( prsslft.toString());

			switch( prsslft.getType())
			{
				case ACK:
					break;
				case REGINNER:
					coord.addInner( e.getChannel(), e.getRemoteAddress());
					break;
				case REGLEAF:
					coord.addLeaf( e.getChannel(), e.getRemoteAddress());
					break;
				case RESULT:
					// TODO get the result to the client
					break;
				case QUERY:
					// TODO get the query to the root node
					coord.query( prsslft);
					break;
				case UNKNOWN:
					break;
			}

			e.getChannel().write(
					new Pressluft( de.tuberlin.dima.presslufthammer.pressluft.Type.ACK,
							new byte[] { (byte) 0 }));
		}
		else
		{
			super.messageReceived( ctx, e);
		}
	}

}

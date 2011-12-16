package de.tuberlin.dima.presslufthammer.network.handler;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChildChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.WriteCompletionEvent;

public class ClientHandler extends SimpleChannelUpstreamHandler {
	
	private Logger logger;
	
	public ClientHandler(Logger logger) {
		this.logger = logger;
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		super.messageReceived(ctx, e);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		logger.error("client : " + e);
		ctx.getChannel().close();
		ctx.sendUpstream(e);
	}
	
	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
		// Log all channel state changes.
		if (e instanceof ChannelStateEvent) {
			logger.trace("client : channel state changed : " + e);
		}
		
		super.handleUpstream(ctx, e);
	}
	
	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		logger.trace("client : open channel : " + e.getChannel());
		super.channelOpen(ctx, e);
	}
	
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		logger.trace("client : channel connected : " + e.getChannel());
		super.channelConnected(ctx, e);
	}
	
	@Override
	public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		logger.trace("client : channel disconnected : " + e.getChannel());
		super.channelDisconnected(ctx, e);
	}
	
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		logger.trace("client : channel closed : " + e.getChannel());
		super.channelClosed(ctx, e);
	}
	
	@Override
	public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
		logger.trace("client : completed writing " + e.getWrittenAmount() + " bytes");
		super.writeComplete(ctx, e);
	}
	
	@Override
	public void childChannelOpen(ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
		logger.trace("client : open child channel : " + e.getChildChannel());
		super.childChannelOpen(ctx, e);
	}
	
	@Override
	public void childChannelClosed(ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
		logger.trace("client : child channel closed : " + e.getChildChannel());
		super.childChannelClosed(ctx, e);
	}
}
package de.tuberlin.dima.presslufthammer.network.handler;

import java.net.SocketAddress;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;

import de.tuberlin.dima.presslufthammer.ontology.Result;
import de.tuberlin.dima.presslufthammer.ontology.Query;
import de.tuberlin.dima.presslufthammer.pressluft.old.Pressluft;

public abstract class ServerHandler extends SimpleChannelHandler {
	
	Logger logger;
	private final ChannelGroup channelGroup;
	
	public ServerHandler(String parent, ChannelGroup channelGroup) {
		this.channelGroup = channelGroup;
		this.logger = Logger.getLogger(parent + " - " + ServerHandler.class.getSimpleName());
	}
	
	public abstract void handleResult(Result data, SocketAddress socketAddress);
	
	public abstract void handleQuery(Query query, Channel ch);
	
	public abstract void handleConnection(Channel ch);
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		if (e.getMessage() instanceof Pressluft) {
			Pressluft prsslft = ((Pressluft) e.getMessage());
			switch (prsslft.getType()) {
				case QUERY:
					handleQuery(Query.fromByteArray(prsslft.getPayload()), e.getChannel());
					break;
				case RESULT:
					handleResult(Result.fromByteArray(prsslft.getPayload()), e.getRemoteAddress());
					break;
				default:
					logger.error("can not handle pressluft : " + prsslft.getType());
					break;
			}
		}
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		logger.error("server : " + e.getCause().getMessage());
		ctx.getChannel().close();
		ctx.sendUpstream(e);
	}
	
	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
		// Log all channel state changes.
		if (e instanceof ChannelStateEvent) {
//			logger.trace("server : channel state changed: " + e);
		}
		
		super.handleUpstream(ctx, e);
	}
	
	@Override
	public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
		// Log all channel state changes.
		if (e instanceof ChannelStateEvent) {
//			logger.trace("server : channel state changed: " + e);
		}
		
		// Sending the event downstream (outbound)
		ctx.sendDownstream(e);
//		super.handleDownstream(ctx, e);
	}
	
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		this.channelGroup.add(e.getChannel());
		handleConnection(e.getChannel());
	}
}
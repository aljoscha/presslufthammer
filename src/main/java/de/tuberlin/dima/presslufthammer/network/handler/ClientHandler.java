package de.tuberlin.dima.presslufthammer.network.handler;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public class ClientHandler extends SimpleChannelUpstreamHandler {
	
	private Logger logger;
	
	public ClientHandler(Logger logger) {
		this.logger = logger;
	}
	
	public ClientHandler() {
		this.logger = Logger.getLogger(ClientHandler.class.getSimpleName());
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		super.messageReceived(ctx, e);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		logger.error(e);
		ctx.getChannel().close();
		ctx.sendUpstream(e);
	}
}
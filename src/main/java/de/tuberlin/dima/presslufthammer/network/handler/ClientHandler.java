package de.tuberlin.dima.presslufthammer.network.handler;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public class ClientHandler extends SimpleChannelUpstreamHandler {
	
	private Logger logger;
	
	public ClientHandler(Logger logger){
		this.logger = logger;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		logger.error(e);
		super.exceptionCaught(ctx, e);
	}
}
package de.tuberlin.dima.presslufthammer.network.handler;

import java.net.SocketAddress;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import de.tuberlin.dima.presslufthammer.ontology.Result;
import de.tuberlin.dima.presslufthammer.ontology.Query;
import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;

public abstract class ServerHandler extends SimpleChannelHandler {
	
	Logger logger;
	
	public ServerHandler(Logger logger) {
		this.logger = logger;
	}
	
	public abstract void handleResult(Result data, SocketAddress socketAddress);
	
	public abstract void handleQuery(Query query);
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		if (e.getMessage() instanceof Pressluft) {
			Pressluft prsslft = ((Pressluft) e.getMessage());
			switch (prsslft.getType()) {
				case QUERY:
					handleQuery(Query.fromByteArray(prsslft.getPayload()));
				case RESULT:
					handleResult(Result.fromByteArray(prsslft.getPayload()), e.getRemoteAddress());
			}
		}
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		logger.error(e);
		ctx.getChannel().close();
		ctx.sendUpstream(e);
	}
}
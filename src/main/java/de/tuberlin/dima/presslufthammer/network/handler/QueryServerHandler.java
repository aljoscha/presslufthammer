package de.tuberlin.dima.presslufthammer.network.handler;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import de.tuberlin.dima.presslufthammer.ontology.Query;
import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;

public abstract class QueryServerHandler extends SimpleChannelHandler {
	
	public abstract void handleMessage(Query query);
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
//		System.err.println("ping : " + e.getMessage());
//		
//		if (e.getMessage() instanceof Query) {
//			System.err.println("pong");
//			handleMessage((Query) e.getMessage());
//		} else {
//			super.messageReceived(ctx, e);
//		}
		
		if (e.getMessage() instanceof Pressluft) {
			Pressluft prsslft = ((Pressluft) e.getMessage());
			switch (prsslft.getType()) {
				case QUERY:
					handleMessage(Query.fromByteArray(prsslft.getPayload()));
			}
		}
	}
}
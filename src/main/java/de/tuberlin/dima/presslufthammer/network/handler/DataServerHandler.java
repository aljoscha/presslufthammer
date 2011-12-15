package de.tuberlin.dima.presslufthammer.network.handler;

import java.net.SocketAddress;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import de.tuberlin.dima.presslufthammer.ontology.Result;
import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;

public abstract class DataServerHandler extends SimpleChannelHandler {
	
	public abstract void handleMessage(Result data, SocketAddress socketAddress);
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
//		if (e.getMessage() instanceof Data) {
//			handleMessage((Data) e.getMessage(), e.getRemoteAddress());
//		} else {
//			super.messageReceived(ctx, e);
//		}
		
		if (e.getMessage() instanceof Pressluft) {
			Pressluft prsslft = ((Pressluft) e.getMessage());
			switch (prsslft.getType()) {
				case RESULT:
					handleMessage(Result.fromByteArray(prsslft.getPayload()), e.getRemoteAddress());
			}
		}
	}
}
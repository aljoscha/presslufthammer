/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

import java.net.ConnectException;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;

/**
 * @author feichh
 * @author Aljoscha Krettek
 * 
 */
public class LeafHandler extends SimpleChannelHandler {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ChannelGroup openChannels;
    private final Leaf leaf;

    public LeafHandler(Leaf leaf, ChannelGroup channelGroup) {
        this.leaf = leaf;
        this.openChannels = channelGroup;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        Throwable cause = e.getCause();
        if (cause instanceof ConnectException) {
            ConnectException connectException = (ConnectException) cause;
            log.error(connectException.getMessage());
            ctx.sendUpstream(e);
        } else {
            super.exceptionCaught(ctx, e);
        }
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        log.debug("Adding channel to openChannels: {}.", e.getChannel()
                .getRemoteAddress());
        this.openChannels.add(e.getChannel());
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
		log.debug("Message received from {}.", e.getRemoteAddress());
		if (e.getMessage() instanceof SimpleMessage) {
			SimpleMessage pressluft = ((SimpleMessage) e.getMessage());
			log.debug("Message: {}", pressluft.toString());
            switch (pressluft.getType()) {
            case ACK:
                break;
            case INFO:
                // InetSocketAddress innerAddress = getSockAddrFromBytes(prsslft
                // .getPayload());
                // // leaf.close();
                // leaf.connectNReg(innerAddress);
                break;
            case QUERY:
                leaf.query(pressluft);
                break;
            case REGINNER:
            case REGLEAF:
            case RESULT:
            case UNKNOWN:
                break;

            }
            // e.getChannel().write(e.getMessage());
        } else {
            super.messageReceived(ctx, e);
        }
    }

    // private InetSocketAddress getSockAddrFromBytes(byte[] payload) {
    // // TODO
    // String temp = new String(payload);
    // log.debug(temp);
    // String[] split = temp.split(":");
    // String ipaddr = split[0].replaceAll("/", "");
    // int port = Integer.parseInt(split[1]) + 1;
    // log.debug(ipaddr + " " + port);
    // return new InetSocketAddress(ipaddr, port);
    // }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
            throws Exception {

        // Log all channel state changes.
        if (e instanceof ChannelStateEvent) {
            log.debug("Channel state changed: " + e);
        }

        super.handleUpstream(ctx, e);
    }
}

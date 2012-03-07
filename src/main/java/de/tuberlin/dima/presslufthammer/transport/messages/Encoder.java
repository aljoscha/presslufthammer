package de.tuberlin.dima.presslufthammer.transport.messages;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

@ChannelHandler.Sharable
public class Encoder extends OneToOneEncoder {
    private final Charset charset;

    public Encoder() {
        this(Charset.defaultCharset());
    }

    public Encoder(Charset charset) {
        if (charset == null) {
            throw new NullPointerException("charset");
        }
        this.charset = charset;
    }

    public static Encoder getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public ChannelBuffer encodeSimple(SimpleMessage message)
            throws IllegalArgumentException {
        // you can move these verifications "upper" (before writing to the
        // channel) in order not to cause a
        // channel shutdown

        if ((message.getType() == null)
                || (message.getType() == MessageType.UNKNOWN)) {
            throw new IllegalArgumentException(
                    "Message type cannot be null or UNKNOWN");
        }

        if ((message.getPayload() == null)
                || (message.getPayload().length == 0)) {
            throw new IllegalArgumentException(
                    "Message payload cannot be null or empty");
        }

        // type(1b) + qid(1b) + payload length(4b) + payload(nb)
        int size = 9 + message.getPayload().length;

        ChannelBuffer buffer = ChannelBuffers.buffer(size);
        buffer.writeByte(message.getType().getByteValue());
        buffer.writeInt(message.getQueryID());
        buffer.writeInt(message.getPayload().length);
        buffer.writeBytes(message.getPayload());

        return buffer;
    }

    public ChannelBuffer encodeQuery(QueryMessage message)
            throws IllegalArgumentException {

        if (message.getQuery() == null) {
            throw new IllegalArgumentException("QueryMessage has no query");
        }

        ChannelBuffer queryBuffer = ChannelBuffers.copiedBuffer(message
                .getQuery().toString(), charset);
        // type(1b) + qid(4b) + payload length(4b) + payload(nb)
        int size = 9 + queryBuffer.capacity();

        ChannelBuffer buffer = ChannelBuffers.buffer(size);

        buffer.writeByte(MessageType.QUERY.getByteValue());
        buffer.writeInt(message.getQueryId());
        buffer.writeInt(queryBuffer.readableBytes());
        buffer.writeBytes(queryBuffer);

        return buffer;
    }

    public ChannelBuffer encodeTablet(TabletMessage message)
            throws IllegalArgumentException {

        if (message.getTabletData() == null) {
            throw new IllegalArgumentException(
                    "TabletMessage has no tablet data query");
        }

        // type(1b) + qid(4b) + payload length(4b) + payload(nb)
        int size = 9 + message.getTabletData().length;

        ChannelBuffer buffer = ChannelBuffers.buffer(size);

        buffer.writeByte(MessageType.TABLET.getByteValue());
        buffer.writeInt(message.getQueryId());
        buffer.writeInt(message.getTabletData().length);
        buffer.writeBytes(message.getTabletData());

        return buffer;
    }

    @Override
    protected Object encode(ChannelHandlerContext channelHandlerContext,
            Channel channel, Object msg) throws Exception {
        if (msg instanceof SimpleMessage) {
            return encodeSimple((SimpleMessage) msg);
        } else if (msg instanceof QueryMessage) {
            return encodeQuery((QueryMessage) msg);
        } else if (msg instanceof TabletMessage) {
            return encodeTablet((TabletMessage) msg);
            // return encodeTablet((TabletMessage) msg);
        } else {
            return msg;
        }
    }

    private static final class InstanceHolder {
        private static final Encoder INSTANCE = new Encoder();
    }
}
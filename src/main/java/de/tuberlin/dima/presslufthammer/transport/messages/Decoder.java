package de.tuberlin.dima.presslufthammer.transport.messages;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;

import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.query.parser.QueryParser;

public class Decoder extends ReplayingDecoder<Decoder.DecodingState> {

    private byte[] buffer;
    private MessageType messageType;
    private int queryId;

    private final Charset charset;

    public Decoder() {
        this(Charset.defaultCharset());
    }

    public Decoder(Charset charset) {
        if (charset == null) {
            throw new NullPointerException("charset");
        }
        this.charset = charset;
        this.reset();
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel,
            ChannelBuffer messageBuffer, DecodingState state) throws Exception {

        switch (state) {
        case TYPE:
            messageType = MessageType.fromByte(messageBuffer.readByte());
            checkpoint(DecodingState.QID);
        case QID:
            queryId = messageBuffer.readInt();
            checkpoint(DecodingState.PAYLOAD_LENGTH);
        case PAYLOAD_LENGTH:
            int size = messageBuffer.readInt();
            if (size <= 0) {
                throw new Exception("Invalid content size");
            }
            // pre-allocate content buffer

            buffer = new byte[size];
            checkpoint(DecodingState.PAYLOAD);
        case PAYLOAD:
            messageBuffer.readBytes(buffer, 0, buffer.length);
            try {
                switch (messageType) {
                case QUERY:
                    ChannelBuffer buf = ChannelBuffers.copiedBuffer(buffer);
                    Query query = QueryParser.parse(buf.toString(charset));
                    QueryMessage msg = new QueryMessage(queryId, query);
                    return msg;
                case TABLET:
                    return new TabletMessage(queryId, buffer);
                default:
                    return new SimpleMessage(messageType, queryId, buffer);
                }
            } finally {
                this.reset();
            }
        default:
            throw new Exception("Unknown decoding state: " + state);
        }
    }

    private void reset() {
        checkpoint(DecodingState.TYPE);
    }

    public enum DecodingState {
        TYPE, QID, PAYLOAD_LENGTH, PAYLOAD,
    }
}
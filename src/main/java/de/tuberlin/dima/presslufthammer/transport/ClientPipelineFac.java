/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import de.tuberlin.dima.presslufthammer.pressluft.Decoder;
import de.tuberlin.dima.presslufthammer.pressluft.Encoder;

/**
 * @author feichh
 * 
 */
public class ClientPipelineFac implements ChannelPipelineFactory {

	private final CLIClient client;

	/**
	 * @param client
	 */
	public ClientPipelineFac(CLIClient client) {
		this.client = client;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jboss.netty.channel.ChannelPipelineFactory#getPipeline()
	 */
	public ChannelPipeline getPipeline() throws Exception {
		// TODO add required ChannelHandlers
		ChannelPipeline pipe = pipeline();

		// // Decoders
		// pipe.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder(
		// 1048576, 0,
		// 4, 0, 4));
		// pipe.addLast( "protobufDecoder", new ProtobufDecoder( null));
		// // MyMessage.getDefaultInstance()));// TODO
		//
		// // Encoder
		// pipe.addLast( "frameEncoder", new LengthFieldPrepender( 4));
		// pipe.addLast( "protobufEncoder", new ProtobufEncoder());

		pipe.addLast("Encoder", Encoder.getInstance());
		pipe.addLast("Decoder", new Decoder());
		pipe.addLast("LeafHandler", new ClientHandler(client));
		return pipe;
	}

}

/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;
import de.tuberlin.dima.presslufthammer.pressluft.Type;

/**
 * @author feichh
 * 
 */
public class Leaf extends ChannelNode {
	private static final Pressluft REGMSG = new Pressluft(Type.REGLEAF,
			(byte) 0, "Hello".getBytes());

	private final Logger log = LoggerFactory.getLogger(getClass());
	
	private Channel parentChannel;

	/**
	 * @param host
	 * @param port
	 */
	public Leaf(String host, int port) {

		if (connectNReg(host, port)) {
			log.info("registered with coordinator");
		}
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.presslufthammer.transport.ChannelNode#connectNReg(java.net.SocketAddress)
	 */
	@Override
	public boolean connectNReg(SocketAddress address) {
		// TODO
		ClientBootstrap bootstrap = new ClientBootstrap(
				new NioClientSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory(new LeafPipelineFac(this));
		ChannelFuture connectFuture = bootstrap.connect(address);

		connectFuture.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {
				parentChannel = future.getChannel();
				openChannels.add(parentChannel);
				parentChannel.write(REGMSG);
			}
		});

		return true;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.presslufthammer.transport.ChannelNode#query(de.tuberlin.dima.presslufthammer.pressluft.Pressluft)
	 */
	@Override
	public void query(Pressluft query) {
		String queryString = new String(query.getPayload());
		log.info("query: " + queryString);

		Pressluft message = new Pressluft(Type.RESULT, query.getQueryID(), queryString
				.toUpperCase().getBytes());

		parentChannel.write(message);
	}

	//
	// /*
	// * (non-Javadoc)
	// *
	// * @see de.tuberlin.dima.presslufthammer.testing.ChannelNode#close()
	// */
	// @Override
	// public void close() throws IOException
	// {
	// // TODO
	// //
	// // channel.close().awaitUninterruptibly();
	// // bootstrap.releaseExternalResources();
	// // parentChannel.close();
	// super.close();
	// }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("hostname port");
    }

    public static void main(String[] args) throws InterruptedException {
        // Print usage if necessary.
        if (args.length < 2) {
            printUsage();
            return;
        }
        // Parse options.
        String host = args[0];
        int port = Integer.parseInt(args[1]);

        Leaf leaf = new Leaf(host, port);
    }
}

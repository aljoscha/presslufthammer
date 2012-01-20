/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;

/**
 * @author feichh
 * 
 */
public class CLIClient extends ChannelNode {
	
	private static final Pressluft REGMSG = new Pressluft(
			de.tuberlin.dima.presslufthammer.pressluft.Type.REGCLIENT,
			(byte) 0, new byte[] { (byte) 7 });

	private final Logger log = LoggerFactory.getLogger(getClass());
	private Channel parentChannel;

	public CLIClient(String host, int port) {

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

		bootstrap.setPipelineFactory(new ClientPipelineFac(this));

		ChannelFuture connectFuture = bootstrap.connect(address);

		parentChannel = connectFuture.awaitUninterruptibly().getChannel();
		ChannelFuture writeFuture = parentChannel.write(REGMSG);

		return writeFuture.awaitUninterruptibly().isSuccess();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.presslufthammer.transport.ChannelNode#query(de.tuberlin.dima.presslufthammer.pressluft.Pressluft)
	 */
	@Override
	public void query(Pressluft query) {

		if (query != null && parentChannel != null
				&& parentChannel.isConnected() && parentChannel.isWritable()) {
			parentChannel.write(query);
		}
	}

	/**
	 * @param prsslft
	 */
	public void handleResult(Pressluft prsslft) {
		// TODO
		log.info("Result received: " + new String(prsslft.getPayload()));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.dima.presslufthammer.testing.ChannelNode#close()
	 */
	@Override
	public void close() throws IOException {
		// TODO
		// channel.close().awaitUninterruptibly();
		// bootstrap.releaseExternalResources();
		parentChannel.disconnect();
		super.close();
	}

	/**
	 * Prints the usage to System.out.
	 */
	private static void printUsage() {
		System.out.println("Usage:");
		System.out.println("hostname port");
	}

	/**
	 * @param args
	 * @throws InterruptedException
	 *             if interrupted
	 * @throws IOException
	 *             if IO goes awry
	 */
	public static void main(String[] args) throws InterruptedException,
			IOException {
		// Print usage if necessary.
		if (args.length < 2) {
			printUsage();
			return;
		}
		// Parse options.
		String host = args[0];
		int port = Integer.parseInt(args[1]);

		CLIClient client = new CLIClient(host, port);
		boolean assange = true;
		// Console console = System.console();
		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(System.in));
		while (assange) {
			String line = bufferedReader.readLine();
			if (line.startsWith("x")) {
				assange = false;
			} else {
				client.query(line);
			}
		}

		client.close();
	}
}

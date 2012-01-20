package de.tuberlin.dima.presslufthammer.transport;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.Type;
import de.tuberlin.dima.presslufthammer.util.ShutdownStopper;
import de.tuberlin.dima.presslufthammer.util.Stoppable;

/**
 * @author feichh
 * @author Aljoscha Krettek
 * 
 */
public class Leaf extends ChannelNode implements Stoppable {
    private static final SimpleMessage REGMSG = new SimpleMessage(Type.REGLEAF,
            (byte) 0, "Hello".getBytes());
    private static int CONNECT_TIMEOUT = 10000;
    private final Logger log = LoggerFactory.getLogger(getClass());

    private Channel coordinatorChannel;
    private ClientBootstrap bootstrap;
    private String serverHost;
    private int serverPort;

    public Leaf(String serverHost, int serverPort) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
    }

    public void start() {
        ChannelFactory factory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        bootstrap = new ClientBootstrap(factory);

        bootstrap.setPipelineFactory(new LeafPipelineFac(this));
        bootstrap.setOption("connectTimeoutMillis", CONNECT_TIMEOUT);

        SocketAddress address = new InetSocketAddress(serverHost, serverPort);

        ChannelFuture connectFuture = bootstrap.connect(address);

        if (connectFuture.awaitUninterruptibly().isSuccess()) {
            coordinatorChannel = connectFuture.getChannel();
            coordinatorChannel.write(REGMSG);
            log.info("Connected to coordinator at {}:{}", serverHost,
                    serverPort);
            Runtime.getRuntime().addShutdownHook(new ShutdownStopper(this));
        } else {
            bootstrap.releaseExternalResources();
            log.info("Failed to conncet to coordinator at {}:{}", serverHost,
                    serverPort);
        }

    }

    @Override
	public void query(SimpleMessage query) {
        String queryString = new String(query.getPayload());
        log.info("Received query: " + queryString);
        try {
        Query test = new Query(query.getPayload());
        } catch(Exception e) {
        	log.error("fail", e);
        }
        SimpleMessage message = new SimpleMessage(Type.RESULT, query.getQueryID(),
                queryString.toUpperCase().getBytes());

        coordinatorChannel.write(message);
    }

	public void query(Query query) {
		// TODO
		log.debug("Query received: " + query);
	}

    @Override
    public void stop() {
        log.info("Stopping leaf.");
        if (coordinatorChannel != null) {
            coordinatorChannel.close().awaitUninterruptibly();
        }
        bootstrap.releaseExternalResources();
        log.info("Leaf stopped.");
    }

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
        leaf.start();
    }
}

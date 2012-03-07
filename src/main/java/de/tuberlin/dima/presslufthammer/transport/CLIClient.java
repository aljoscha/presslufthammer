package de.tuberlin.dima.presslufthammer.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.query.parser.QueryParser;
import de.tuberlin.dima.presslufthammer.transport.messages.QueryMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;

/**
 * @author feichh
 * @author Aljoscha Krettek
 * 
 */

public class CLIClient extends ChannelNode {

    private static final SimpleMessage REGMSG = new SimpleMessage(
            de.tuberlin.dima.presslufthammer.transport.messages.MessageType.REGCLIENT,
            (byte) 0, new byte[] { (byte) 7 });

    private final Logger log = LoggerFactory.getLogger(getClass());

    private String coordinatorHost;
    private int coordinatorPort;
    private Channel coordinatorChannel;
    private ClientBootstrap bootstrap;

    public CLIClient(String coordinatorHost, int coordinatorPort) {
        this.coordinatorHost = coordinatorHost;
        this.coordinatorPort = coordinatorPort;
    }

    public boolean start() {
        ChannelFactory factory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        bootstrap = new ClientBootstrap(factory);

        bootstrap.setPipelineFactory(new GenericPipelineFac(this));

        SocketAddress address = new InetSocketAddress(coordinatorHost,
                coordinatorPort);

        ChannelFuture connectFuture = bootstrap.connect(address);

        if (connectFuture.awaitUninterruptibly().isSuccess()) {
            coordinatorChannel = connectFuture.getChannel();
            ChannelFuture writeFuture = coordinatorChannel.write(REGMSG);
            writeFuture.awaitUninterruptibly();
            log.info("Connected to coordinator at {}:{}", coordinatorHost,
                    coordinatorPort);
            return true;
        } else {
            bootstrap.releaseExternalResources();
            log.info("Failed to conncet to coordinator at {}:{}",
                    coordinatorHost, coordinatorPort);
            return false;
        }
    }

    public void query(QueryMessage query) {
        if (query != null && coordinatorChannel != null
                && coordinatorChannel.isConnected()
                && coordinatorChannel.isWritable()) {
            coordinatorChannel.write(query);
        }
    }

    public void handleResult(SimpleMessage message) {
        String resultString = new String(message.getPayload());
        System.out.println(resultString);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {

        log.debug("Message received from {}.", e.getRemoteAddress());
        if (e.getMessage() instanceof SimpleMessage) {
            SimpleMessage message = ((SimpleMessage) e.getMessage());
            log.debug("Message: {}", message.toString());
            switch (message.getType()) {
            case ACK:
            case INFO:
            case INTERNAL_QUERY:
            case REGINNER:
            case REGLEAF:
                break;
            case CLIENT_RESULT:
                this.handleResult(message);
            case UNKNOWN:
                break;
            }
        }
    }

    public void stop() throws IOException {
        if (coordinatorChannel != null) {
            coordinatorChannel.disconnect().awaitUninterruptibly();
        }
        bootstrap.releaseExternalResources();
    }

    /**
     * Prints the usage to System.out.
     */
    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("hostname port");
    }

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
        boolean running = client.start();

        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(System.in));

        while (running) {
            String line = bufferedReader.readLine();
            if (line.startsWith("x")) {
                running = false;
            } else {

                Query query = QueryParser.parse(line);
                System.out.println("QUERY: " + query);
                QueryMessage queryMsg = new QueryMessage(-1, query);
                client.query(queryMsg);
            }
        }

        client.stop();
    }
}

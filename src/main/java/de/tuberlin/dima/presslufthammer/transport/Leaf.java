package de.tuberlin.dima.presslufthammer.transport;

import java.io.File;
import java.io.IOException;
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

import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryWriteonlyTablet;
import de.tuberlin.dima.presslufthammer.data.columnar.local.LocalDiskDataStore;
import de.tuberlin.dima.presslufthammer.qexec.QueryExecutor;
import de.tuberlin.dima.presslufthammer.qexec.QueryHelper;
import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.transport.messages.MessageType;
import de.tuberlin.dima.presslufthammer.transport.messages.QueryMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;
import de.tuberlin.dima.presslufthammer.transport.util.GenericPipelineFac;
import de.tuberlin.dima.presslufthammer.transport.messages.TabletMessage;
import de.tuberlin.dima.presslufthammer.util.ShutdownStopper;
import de.tuberlin.dima.presslufthammer.util.Stoppable;

/**
 * @author feichh
 * @author Aljoscha Krettek
 * 
 */
public class Leaf extends ChannelNode implements Stoppable {
    private static final SimpleMessage REGMSG = new SimpleMessage(
            MessageType.REGLEAF, (byte) 0, "Hello".getBytes());
    private static int CONNECT_TIMEOUT = 10000;
    private final Logger log = LoggerFactory.getLogger(getClass());

    private Channel coordinatorChannel;
    private ClientBootstrap bootstrap;
    private String serverHost;
    private int serverPort;

    private LocalDiskDataStore dataStore;

    private boolean running = false;

    public Leaf(String serverHost, int serverPort, File dataDirectory) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;

        try {
            dataStore = LocalDiskDataStore.openDataStore(dataDirectory);
        } catch (IOException e) {
            log.warn("Exception caught while while loading datastore: {}",
                    e.getMessage());
        }
    }

    public void start() {
        ChannelFactory factory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        bootstrap = new ClientBootstrap(factory);

        bootstrap.setPipelineFactory(new GenericPipelineFac(this));
        bootstrap.setOption("connectTimeoutMillis", CONNECT_TIMEOUT);

        SocketAddress address = new InetSocketAddress(serverHost, serverPort);

        ChannelFuture connectFuture = bootstrap.connect(address);

        if (connectFuture.awaitUninterruptibly().isSuccess()) {
            coordinatorChannel = connectFuture.getChannel();
            coordinatorChannel.write(REGMSG);
            log.info("Connected to coordinator at {}:{}", serverHost,
                    serverPort);
            Runtime.getRuntime().addShutdownHook(new ShutdownStopper(this));
            running = true;
        } else {
            bootstrap.releaseExternalResources();
            log.info("Failed to conncet to coordinator at {}:{}", serverHost,
                    serverPort);
        }

    }

    public void query(QueryMessage message, Channel channel) {
        Query query = message.getQuery();
        log.info("Received query \"{}\" from {}", query,
                channel.getRemoteAddress());

        String tableName = query.getTableName();

        try {
            Tablet tablet = dataStore
                    .getTablet(tableName, query.getPartition());

            log.debug("Tablet: {}:{}", tablet.getSchema().getName(),
                    query.getPartition());

            QueryHelper helper = new QueryHelper(query, tablet.getSchema());
            QueryExecutor qx = new QueryExecutor(helper);

            qx.performQuery(tablet);
            qx.finalizeGroups();

            InMemoryWriteonlyTablet resultTablet = qx.getResultTablet();

            TabletMessage response = new TabletMessage(message.getQueryId(),
                    resultTablet.serialize());

            channel.write(response);
        } catch (IOException e) {
            log.warn("Caught exception while performing query: {}", e);
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        log.debug("Message received from {}.", e.getRemoteAddress());
        if (e.getMessage() instanceof QueryMessage) {
            query((QueryMessage) e.getMessage(), e.getChannel());
        } else if (e.getMessage() instanceof SimpleMessage) {
            SimpleMessage simpleMsg = ((SimpleMessage) e.getMessage());
            log.debug("Message: {}", simpleMsg.toString());
            switch (simpleMsg.getType()) {
            case ACK:
                break;
            case REDIR:
                // InetSocketAddress innerAddress = getSockAddrFromBytes(pressluft
                break;
            case REGINNER:
            case REGLEAF:
            case INTERNAL_RESULT:
            case UNKNOWN:
                break;

            }
        }
    }

    @Override
    public void stop() {
        if (running) {
            log.info("Stopping leaf.");
            if (coordinatorChannel != null) {
                coordinatorChannel.close().awaitUninterruptibly();
            }
            bootstrap.releaseExternalResources();
            log.info("Leaf stopped.");
            running = false;
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("hostname port data-dir");
    }

    public static void main(String[] args) throws InterruptedException {
        // Print usage if necessary.
        if (args.length < 3) {
            printUsage();
            return;
        }
        // Parse options.
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        File dataDirectory = new File(args[2]);

        Leaf leaf = new Leaf(host, port, dataDirectory);
        leaf.start();
    }
}

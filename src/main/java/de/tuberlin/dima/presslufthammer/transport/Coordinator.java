package de.tuberlin.dima.presslufthammer.transport;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.query.SelectClause;
import de.tuberlin.dima.presslufthammer.transport.messages.MessageType;
import de.tuberlin.dima.presslufthammer.transport.messages.QueryMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;
import de.tuberlin.dima.presslufthammer.transport.util.GenericHandler;
import de.tuberlin.dima.presslufthammer.transport.util.GenericPipelineFac;
import de.tuberlin.dima.presslufthammer.transport.util.QueryHandler;
import de.tuberlin.dima.presslufthammer.transport.messages.TabletMessage;
import de.tuberlin.dima.presslufthammer.util.ShutdownStopper;
import de.tuberlin.dima.presslufthammer.util.Stoppable;
import de.tuberlin.dima.presslufthammer.xml.DataSource;
import de.tuberlin.dima.presslufthammer.xml.DataSourcesReader;
import de.tuberlin.dima.presslufthammer.xml.DataSourcesReaderImpl;

/**
 * @author feichh
 * @author Aljoscha Krettek
 * 
 */
public class Coordinator extends ChannelNode implements Stoppable {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private int port;
    private ServerBootstrap bootstrap;
    private Channel acceptChannel;
    private ChannelGroup innerChannels = new DefaultChannelGroup();
    private ChannelGroup leafChannels = new DefaultChannelGroup();
    private ChannelGroup clientChannels = new DefaultChannelGroup();
    private final GenericHandler handler = new GenericHandler(this);
    private Channel rootChannel = null;
    private final Map<Integer, QueryHandler> queries = new HashMap<Integer, QueryHandler>();
    private Map<String, DataSource> tables;
    private int priorQID = -1;

    private boolean running = false;

    public Coordinator(int port, String dataSources) {
        this.port = port;
        DataSourcesReader dsReader = new DataSourcesReaderImpl();
        try {
            tables = dsReader.readFromXML(dataSources);
            log.info("Read datasources from {}.", dataSources);
            log.info(tables.toString());
        } catch (Exception e) {
            log.warn("Error reading datasources from {}: {}", dataSources,
                    e.getMessage());
            if (tables == null) {
                tables = Maps.newHashMap();
            }
        }
    }

    public void start() {
        ChannelFactory factory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        bootstrap = new ServerBootstrap(factory);

        bootstrap.setPipelineFactory(new GenericPipelineFac(this.handler));

        try {
            acceptChannel = bootstrap.bind(new InetSocketAddress(port));
        } catch (ChannelException ce) {
            Throwable cause = ce.getCause();
            log.error("Failed to start coordinator at port {}: {}.", port,
                    cause.getMessage());
            return;
        }

        if (acceptChannel.isBound()) {
            Runtime.getRuntime().addShutdownHook(new ShutdownStopper(this));
            log.info("Coordinator launched at: " + port);
            running = true;
        } else {
            bootstrap.releaseExternalResources();
            log.error("Failed to start coordinator at port {}.", port);
        }

    }

    @Override
    public void stop() {
        if (running) {
            log.info("Stopping coordinator.");
            this.openChannels.close().awaitUninterruptibly();
            bootstrap.releaseExternalResources();
            log.info("Coordinator stopped.");
            running = false;
        }
    }

    public void query(QueryMessage message, Channel client) {
        log.info("Received query \"{}\" from client {}.", message.getQuery(),
                client.getRemoteAddress());

        if (isServing()) {
            if (rootChannel != null) {
                log.info("Handing query to root node of our node tree.");
                // // clientChans.add(client);// optional
                // byte qid = nextQID();
                // message.setQueryID(qid);
                // queries.put(qid, new QueryHandler(1, message, client));
                // rootChannel.write(message);

            } else {
                log.info("Querying leafs directly.");
                int qid = nextQID();
                message = new QueryMessage(qid, message.getQuery());
                Query query = message.getQuery();

                String tableName = query.getTableName();
                if (!tables.containsKey(tableName)) {
                    client.write(new SimpleMessage(MessageType.CLIENT_RESULT,
                            (byte) -1, "Table not available".getBytes()));
                    log.info("Table {} not in tables.", tableName);
                } else {
                    DataSource table = tables.get(tableName);
                    Set<String> projectedFields = Sets.newHashSet();
                    for (SelectClause selectClause : query.getSelectClauses()) {
                        projectedFields.add(selectClause.getColumn());
                    }
                    SchemaNode projectedSchema = null;
                    if (projectedFields.contains("*")) {
                        log.debug("Query is a 'select * ...' query.");
                        projectedSchema = table.getSchema();
                    } else {
                        projectedSchema = table.getSchema().project(
                                projectedFields);
                    }
                    queries.put(qid, new QueryHandler(table.getNumPartitions(),
                            message, projectedSchema, client));

                    // send a request to the leafs for every partition
                    Iterator<Channel> leafIter = leafChannels.iterator();
                    for (int i = 0; i < table.getNumPartitions(); ++i) {
                        Channel leaf = null;
                        if (leafIter.hasNext()) {
                            leaf = leafIter.next();
                        } else {

                            leafIter = leafChannels.iterator();
                            leaf = leafIter.next();
                        }
                        // create a new query for only the specific partition
                        Query leafQuery = new Query(query);
                        leafQuery.setPartition(i);
                        QueryMessage leafMessage = new QueryMessage(qid,
                                leafQuery);
                        leaf.write(leafMessage);
                        log.info("Sent query to leaf {}: {}",
                                leaf.getRemoteAddress(), leafQuery);
                    }
                }
            }

        } else {
            log.warn("Query cannot be processed because we have no leafs.");
        }
    }

    /**
     * Returns {@code true} if this coordinator is connected to at least one
     * leaf.
     */
    public boolean isServing() {
        return handler != null && !leafChannels.isEmpty();
    }

    public void addClient(Channel channel) {
        // TODO
        log.debug("Adding client channel: {}.", channel.getRemoteAddress());
        clientChannels.add(channel);
    }

    public void addInner(Channel channel) {
        // TODO
        log.info("Adding inner node: {}", channel.getRemoteAddress());
        innerChannels.add(channel);
        if (rootChannel == null) {
            log.debug("new root node connected.");
            rootChannel = channel;
            SimpleMessage rootInfo = getRootInfo();
            for (Channel chan : leafChannels) {
                chan.write(rootInfo);
            }
        }
    }

    public void addLeaf(Channel channel) {
        // TODO
        log.info("Adding leaf node: {}", channel.getRemoteAddress());
        leafChannels.add(channel);
        if (rootChannel != null) {
            channel.write(getRootInfo());
        }
    }

    private SimpleMessage getRootInfo() {
        // TODO
        MessageType type = MessageType.REDIR;
        byte[] payload = rootChannel.getRemoteAddress().toString().getBytes();
        return new SimpleMessage(type, (byte) 0, payload);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * de.tuberlin.dima.presslufthammer.transport.ChannelNode#removeChannel(
     * org.jboss.netty.channel.Channel)
     */
    @Override
    public void removeChannel(Channel channel) {
        // TODO
        if (rootChannel == channel) {
            rootChannel = null;
        }
        channel.close();
        // log.debug( "" + openChannels.remove( channel));
    }

    public void handleResult(TabletMessage message) {
        // TODO
        int qid = message.getQueryId();
        QueryHandler qhand = queries.get(qid);
        System.out.println(queries);
        if (qhand != null) {
            qhand.addPart(message);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * de.tuberlin.dima.presslufthammer.transport.ChannelNode#messageReceived
     * (org.jboss.netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.MessageEvent)
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        log.debug("Message received from {}.", e.getRemoteAddress());
        if (e.getMessage() instanceof QueryMessage) {
            query((QueryMessage) e.getMessage(), e.getChannel());
        } else if (e.getMessage() instanceof TabletMessage) {
            handleResult((TabletMessage) e.getMessage());
        } else if (e.getMessage() instanceof SimpleMessage) {
            SimpleMessage message = ((SimpleMessage) e.getMessage());
            log.debug("Message: {}", message.toString());

            switch (message.getType()) {
            case ACK:
                break;
            case REGINNER:
                this.addInner(e.getChannel());
                break;
            case REGLEAF:
                this.addLeaf(e.getChannel());
                break;
            case UNKNOWN:
                break;
            case REDIR:
                break;
            case REGCLIENT:
                this.addClient(e.getChannel());
                break;
            }

            e.getChannel().write(
                    new SimpleMessage(MessageType.ACK, (byte) 0,
                            new byte[] { (byte) 0 }));
        }
    }

    private int nextQID() {
        return ++priorQID;
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("port datasources");
    }

    public static void main(String[] args) {
        // Print usage if necessary.
        if (args.length < 2) {
            printUsage();
            return;
        }

        int port = Integer.parseInt(args[0]);
        String datasources = args[1];

        Coordinator coordinator = new Coordinator(port, datasources);
        coordinator.start();
    }
}

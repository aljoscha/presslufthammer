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
import de.tuberlin.dima.presslufthammer.transport.messages.TabletMessage;
import de.tuberlin.dima.presslufthammer.transport.util.GenericHandler;
import de.tuberlin.dima.presslufthammer.transport.util.GenericPipelineFac;
import de.tuberlin.dima.presslufthammer.transport.util.QueryHandler;
import de.tuberlin.dima.presslufthammer.transport.util.ServingChannel;
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
public class SlaveCoordinator extends ChannelNode implements Stoppable {
	private final Logger log = LoggerFactory.getLogger(getClass());

	private int port;
	private ServerBootstrap bootstrap;
	private Channel acceptChannel;
	private ChannelGroup slaveChannels = new DefaultChannelGroup();
	private ChannelGroup clientChannels = new DefaultChannelGroup();
	private final GenericHandler handler = new GenericHandler(this);
	private ServingChannel rootChannel = null;
	private final Map<Byte, QueryHandler> queries = new HashMap<Byte, QueryHandler>();
	private Map<String, DataSource> tables;
	private byte priorQID = 0;

	public SlaveCoordinator(int port, String dataSources) {
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
		} else {
			bootstrap.releaseExternalResources();
			log.error("Failed to start coordinator at port {}.", port);
		}

	}

	@Override
	public void stop() {
		log.info("Stopping coordinator.");
		this.openChannels.disconnect().awaitUninterruptibly();
		this.openChannels.close().awaitUninterruptibly();
		bootstrap.releaseExternalResources();
		log.info("Coordinator stopped.");
	}

	public void query(QueryMessage message, Channel client) {
		log.info("Received query({}) from client {}.", message,
				client.getLocalAddress());

		if (isServing()) {

			byte qid = nextQID();
			Query query = message.getQuery();
			message = new QueryMessage(qid, query);
			log.info("Received query: {}", query);

			String tableName = query.getTableName();
			if (!tables.containsKey(tableName)) {
				client.write(new SimpleMessage(MessageType.CLIENT_RESULT, (byte) -1,
						"Table not available".getBytes()));
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
					projectedSchema = table.getSchema()
							.project(projectedFields);
				}

				if (rootChannel != null) {
					log.info("Handing query to root node of our node tree.");
					// clientChans.add(client);// optional
					queries.put(qid, new QueryHandler(table.getNumPartitions(),
							message, projectedSchema, client));
//					// send a seperate query for each tablet
//					for (int i = 0; i < table.getNumPartitions(); i++) {
//						query.setPart((byte) i);
//						message.setPayload(query.getBytes());
//						rootChannel.write(message);
//					}
					// send a request to the leafs for every partition
                    Iterator<Channel> leafIter = slaveChannels.iterator();
                    for (int i = 0; i < table.getNumPartitions(); ++i) {
                        Channel leaf = null;
                        if (leafIter.hasNext()) {
                            leaf = leafIter.next();
                        } else {

                            leafIter = slaveChannels.iterator();
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
				} else {
					log.info("Cannot process query w/o at least one slave.");
				}
			}

		} else {
			log.warn("Query cannot be processed because we have no slaves.");
		}
	}

	/**
	 * Returns {@code true} if this coordinator is connected to at least one
	 * slave.
	 */
	public boolean isServing() {
		return handler != null && !slaveChannels.isEmpty();
	}

	public void addClient(Channel channel) {
		// TODO
		log.debug("Adding client channel: {}.", channel.getRemoteAddress());
		clientChannels.add(channel);
	}

	public void addInner(Channel channel, byte[] portbs) {
		// TODO
		log.info("Adding inner node: {};", channel.getRemoteAddress());
		synchronized (slaveChannels) {
//			log.debug("lock obtained");
			if(slaveChannels.add(channel)) {
				log.debug("addition successful");
			}
			if (rootChannel == null) {
				rootChannel = new ServingChannel(channel, portbs);
				log.info("new root node connected at "
						+ rootChannel.getRemoteAddress());
			} else {
				channel.write(getRootInfo());
//				System.out.println("CCCCCCCCC");
			}
		}
//		log.debug("lock released");
	}

	private SimpleMessage getRootInfo() {
		MessageType type = MessageType.REDIR;
		byte[] payload = rootChannel.getServingAddress().toString().getBytes();
		return new SimpleMessage(type, -1, payload);
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
		slaveChannels.remove(channel);
		channel.close();
		// log.debug( "" + openChannels.remove( channel));
	}

	public void handleResult(TabletMessage message) {
		// TODO
		int qid = message.getQueryId();
		QueryHandler qhand = queries.get(qid);
		if (qhand != null) {
			qhand.addPart(message);
		} else {
			log.error("Received result for query " + qid
					+ " but no handler was found.");
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
				this.addInner(e.getChannel(), message.getPayload());
				break;
			case REGLEAF:
				this.addInner(e.getChannel(), message.getPayload());
				break;
			case INTERNAL_RESULT:
			case CLIENT_QUERY:
			case REDIR:
			case UNKNOWN:
				break;
			case REGCLIENT:
				this.addClient(e.getChannel());
				break;
			}
			//
			// e.getChannel().write(
			// new SimpleMessage(Type.ACK, (byte) 0,
			// new byte[] { (byte) 0 }));
		}
	}

	private byte nextQID() {
		return ++priorQID;
	}

	private static void printUsage() {
		System.out.println("Parameters:");
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

		SlaveCoordinator coordinator = new SlaveCoordinator(port, datasources);
		coordinator.start();
	}
}

package de.tuberlin.dima.presslufthammer.transport;

import java.io.File;
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
import de.tuberlin.dima.presslufthammer.qexec.QueryHelper;
import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.query.SelectClause;
import de.tuberlin.dima.presslufthammer.query.sema.SemaError;
import de.tuberlin.dima.presslufthammer.query.sema.SemanticChecker;
import de.tuberlin.dima.presslufthammer.transport.messages.MessageType;
import de.tuberlin.dima.presslufthammer.transport.messages.QueryMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.TabletMessage;
import de.tuberlin.dima.presslufthammer.transport.util.GenericHandler;
import de.tuberlin.dima.presslufthammer.transport.util.GenericPipelineFac;
import de.tuberlin.dima.presslufthammer.transport.util.ServingChannel;
import de.tuberlin.dima.presslufthammer.util.Config;
import de.tuberlin.dima.presslufthammer.util.ShutdownStopper;
import de.tuberlin.dima.presslufthammer.util.Stoppable;
import de.tuberlin.dima.presslufthammer.util.Config.TableConfig;

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
	private final Map<Integer, QueryHandler> queries = new HashMap<Integer, QueryHandler>();
	private Map<String, TableConfig> tables;
	private byte priorQID = 0;
	private Config config;

	public SlaveCoordinator(int port, String configFile) {
		this.port = port;
		readConfig(configFile);
	}

	public void readConfig(String configFile) {

		try {
			config = new Config(new File(configFile));
			tables = config.getTables();
			log.info("Read config from {}.", configFile);
			log.info(tables.toString());
		} catch (Exception e) {
			log.warn("Error reading config from {}: {}", configFile,
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

			int qid = nextQID();
			Query query = message.getQuery();
			message = new QueryMessage(qid, query);
			log.info("Received query: {}", query);

			String tableName = query.getTableName();
			if (!tables.containsKey(tableName)) {
				client.write(new SimpleMessage(MessageType.CLIENT_RESULT,
						(byte) -1, "Table not available".getBytes()));
				log.info("Table {} not in tables.", tableName);
			} else {
				SemanticChecker sema = new SemanticChecker();
				try {
					sema.checkQuery(query, tables);
				} catch (SemaError e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				TableConfig table = tables.get(tableName);
				QueryHelper queryHelper = new QueryHelper(query,
						table.getSchema());

				if (rootChannel != null) {
					log.info("Handing query {} to root node of the tree.", qid);

					queries.put(qid,
							new QueryHandler(1, message.getQueryId(),
									queryHelper.getRewrittenQuery(),
									queryHelper.getResultSchema(), client));
					// send a query for every partition
					for (int i = 0; i < table.getNumPartitions(); ++i) {
						// create a new query for only the specific partition
						Query leafQuery = new Query(query);
						leafQuery.setPartition(i);
						QueryMessage leafMessage = new QueryMessage(qid,
								leafQuery);
						rootChannel.write(leafMessage);
						log.info("Sent query to root {}: {}",
								rootChannel.getRemoteAddress(), leafQuery);
					}
				} else {
					log.info("Cannot process query w/o at least one slave.");
				}
			}
		} else {
			log.warn("Query cannot be processed w/o slaves.");
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
			if (slaveChannels.add(channel)) {
				log.debug("New node added successfully.");
			}
			if (rootChannel == null) {
				rootChannel = new ServingChannel(channel, portbs);
				log.info("New root node connected at {}.",
						rootChannel.getRemoteAddress());
			} else {
				channel.write(getRootInfo());
			}
		}
	}

	private SimpleMessage getRootInfo() {
		MessageType type = MessageType.REDIR;
		byte[] payload = rootChannel.getServingAddress().toString().getBytes();
		return new SimpleMessage(type, -1, payload);
	}

	@Override
	public void removeChannel(Channel channel) {
		// TODO
		if (rootChannel == channel) {
			rootChannel = null;
		}
		slaveChannels.remove(channel);
		channel.close();
		log.debug("" + openChannels.remove(channel));
	}

	public void handleResult(TabletMessage message) {
		// TODO
		int qid = message.getQueryId();
		QueryHandler qhand = queries.get(qid);
		if (qhand != null) {
			qhand.client.write(new SimpleMessage(MessageType.CLIENT_RESULT,
					message.getQueryId(), message.getTabletData()));
			// qhand.addPart(message);
		} else {
			log.error("Received result for query " + qid
					+ " but no handler was found.");
		}
	}

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
				int qid = message.getQueryID();
				QueryHandler qhand = queries.get(qid);
				if (qhand != null) {
					qhand.client.write(new SimpleMessage(
							MessageType.CLIENT_RESULT, qid, message
									.getPayload()));
				}
				break;
			case CLIENT_QUERY:
			case REDIR:
			case UNKNOWN:
				e.getChannel().write(
						new SimpleMessage(MessageType.NACK, message
								.getQueryID(), new byte[] { (byte) 0 }));
				break;
			case REGCLIENT:
				this.addClient(e.getChannel());
				break;
			}
		}
	}

	private int nextQID() {
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

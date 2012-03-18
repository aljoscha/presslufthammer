package de.tuberlin.dima.presslufthammer.transport;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryWriteonlyTablet;
import de.tuberlin.dima.presslufthammer.data.columnar.local.LocalDiskDataStore;
import de.tuberlin.dima.presslufthammer.qexec.QueryExecutor;
import de.tuberlin.dima.presslufthammer.qexec.QueryHelper;
import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.transport.messages.MessageType;
import de.tuberlin.dima.presslufthammer.transport.messages.QueryMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.TabletMessage;
import de.tuberlin.dima.presslufthammer.transport.util.GenericPipelineFac;
import de.tuberlin.dima.presslufthammer.transport.util.ServingChannel;
import de.tuberlin.dima.presslufthammer.util.Config;
import de.tuberlin.dima.presslufthammer.util.ShutdownStopper;
import de.tuberlin.dima.presslufthammer.util.Stoppable;
import de.tuberlin.dima.presslufthammer.util.Config.TableConfig;

/**
 * ChannelNode that can serve both as an intermediate server or a leaf server.
 * 
 * @author feichh
 * @author Aljoscha Krettek
 * 
 */
public class Slave extends ChannelNode implements Stoppable {

	/**
	 * enumeration representing the current state of the Node
	 */
	public enum NodeStatus {
		STARTUP, INNER, LEAF
	}

	private static final int CONNECT_TIMEOUT = 10000;
	/**
	 * logger that logs loggable stuff
	 */
	private final Logger log = LoggerFactory.getLogger(getClass());

	/**
	 * channel connected to the coordinator
	 */
	private Channel coordinatorChannel;
	/**
	 * channel connected to the current parent
	 */
	private Channel parentChannel;
	/**
	 * channel that accepts incoming connections
	 */
	private Channel servingChannel;
	/**
	 * for connecting to the coordinator
	 */
	private ClientBootstrap bootstrapCoordinator;
	/**
	 * for the connection to the parent
	 */
	private ClientBootstrap bootstrapParent;
	/**
	 * for incoming connections of children
	 */
	private ServerBootstrap bootstrapServer;
	/**
	 * host name or address of the coordinator server
	 */
	private final String serverHost;
	/**
	 * port of the coordinator server
	 */
	private final int serverPort;
	/**
	 * port this slave is listening for clients (children) on
	 */
	private int ownPort;
	/**
	 * number of direct children the slave accepts
	 */
	private final int degree;
	/**
	 * number of children added
	 */
	private int childrenAdded = 0;
	/**
	 * the children directly connected to this slave
	 */
	private List<ServingChannel> directChildren = new ArrayList<ServingChannel>();
	/**
	 * group of all children's channels currently connected
	 */
	private ChannelGroup childChannels = new DefaultChannelGroup();
	/**
	 * the current status of the slave
	 */
	private NodeStatus status = NodeStatus.STARTUP;
	/**
	 * the data store used for data access
	 */
	private LocalDiskDataStore dataStore;
	/**
	 * the configs of all available tables as defined in config.json
	 */
	private Map<String, TableConfig> tables;
	/**
	 * true if the slave is connecting to a new parent.
	 */
	private boolean connecting = false;
	/**
	 * the Config Object to load the config.json file
	 */
	private Config config;
	private Map<Integer, SlaveQueryHandler> queries = Maps.newHashMap();

	/**
	 * Constructor<br />
	 * Opens a data store in the data directory.
	 * 
	 * @param degree
	 *            the number of direct children the slave should accept
	 * @param serverHost
	 *            coordinator host name or address
	 * @param serverPort
	 *            coordinator port
	 * @param dataDirectory
	 *            directory containing data sources
	 * @param configFile
	 *            path to config.json
	 */
	public Slave(int degree, String serverHost, int serverPort,
			File dataDirectory, String configFile) {
		if (degree < 1) {
			log.error("not a valid degree: " + degree, new Exception());
		}
		this.serverHost = serverHost;
		this.serverPort = serverPort;
		this.degree = degree;

		readConfig(configFile);
		dataStore = openDataStore(dataDirectory);
	}

	/**
	 * Constructor<br />
	 * Opens a data store in the data directory.
	 * 
	 * @param degree
	 *            the number of direct children the slave should accept
	 * @param serverHost
	 *            coordinator host name or address
	 * @param serverPort
	 *            coordinator port
	 * @param dataDirectory
	 *            directory containing data sources
	 * @param dataSources
	 *            path to DataSources.xml
	 */
	public Slave(int degree, String serverHost, int serverPort,
			String dataDirectory, String dataSources) {
		if (degree < 1) {
			log.error("not a valid degree: " + degree, new Exception());
		}
		this.serverHost = serverHost;
		this.serverPort = serverPort;
		this.degree = degree;

		readConfig(dataSources);
		dataStore = openDataStore(dataDirectory);
	}

	/**
	 * Wrapper.
	 * 
	 * @param dataDirectory
	 *            path as a String
	 * @return data store at the given directory
	 */
	private LocalDiskDataStore openDataStore(String dataDirectory) {
		return openDataStore(new File(dataDirectory));
	}

	/**
	 * Opens a LocalDiskDataStore at the given directory.
	 * 
	 * @param dataDirectory
	 *            File object
	 * @return data store at the given directory
	 */
	private LocalDiskDataStore openDataStore(File dataDirectory) {
		try {
			return LocalDiskDataStore.openDataStore(dataDirectory);
		} catch (IOException e) {
			log.warn("Exception caught while while loading datastore: {}",
					e.getMessage());
			return null;
		}
	}

	/**
	 * Reads the configuration from a JSON file expected at the given path.
	 * Fills the tables map using this configuration.
	 * 
	 * @param configFile
	 *            path to the JSON configuration file (config.json)
	 */
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

	/**
	 * Sets up a server socket listening for client connections. Connects to the
	 * coordinator using the connection info supplied to the constructor.
	 * Changes ownPort.
	 */
	public void start() {
		ownPort = 0;
		serve();
		ChannelFactory factory = new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		bootstrapCoordinator = new ClientBootstrap(factory);

		bootstrapCoordinator.setPipelineFactory(new GenericPipelineFac(this));
		bootstrapCoordinator.setOption("connectTimeoutMillis", CONNECT_TIMEOUT);

		SocketAddress address = new InetSocketAddress(serverHost, serverPort);
		ChannelFuture connectFuture = bootstrapCoordinator.connect(address);
		// use a listener because awaitUninter... fails on multiple
		// conns/threads
		connectFuture.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {
				coordinatorChannel = future.getChannel();
				// parentChannel = coordinatorChannel;
				openChannels.add(coordinatorChannel);
				coordinatorChannel.write(getRegMsg());
				log.info("Connected to coordinator at {}",
						coordinatorChannel.getRemoteAddress());
				status = NodeStatus.LEAF;
			}
		});
		Runtime.getRuntime().addShutdownHook(new ShutdownStopper(this));
	}

	/**
	 * Binds a socket to a port to listen for client connections and stores the
	 * port in ownPort.
	 */
	public void serve() {
		// Configure the server.
		// ServerBootstrap bootstrap = new ServerBootstrap(
		bootstrapServer = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		// Set up the event pipeline factory.
		bootstrapServer.setPipelineFactory(new GenericPipelineFac(this));

		// Bind and start to accept incoming connections.
		try {
			servingChannel = bootstrapServer
					.bind(new InetSocketAddress(ownPort));
			ownPort = getPortFromSocketAddress(servingChannel.getLocalAddress());
			log.info("serving on port: " + ownPort);
		} catch (org.jboss.netty.channel.ChannelException e) {
			log.warn("failed to bind.");
		}
	}

	/**
	 * Tries to read the port from a {@link SocketAddress}. Sadly there is no
	 * functionality for this in {@link SocketAddress}.
	 * 
	 * @param localAddress
	 *            address to read the port from.
	 * @return the last element of the String[] resulting from split()ing by
	 *         colon
	 */
	private int getPortFromSocketAddress(SocketAddress localAddress) {
		String s = localAddress.toString();
		String[] temp = s.split(":");
		// assuming an IPv4 address
		return Integer.parseInt(temp[temp.length - 1]);
	}

	/**
	 * Handles with incoming queries depending on status.
	 * 
	 * @param message
	 *            QueryMessage
	 */
	public void query(QueryMessage message) {
		Query query = message.getQuery();
		log.info("Received query: " + query);

		switch (status) {
		case INNER:
			// TODO rewriting of query
			int qid = message.getQueryId();
			Channel client = (parentChannel != null) ? parentChannel
					: coordinatorChannel;

			QueryHelper queryHelper = new QueryHelper(query, tables.get(
					query.getTableName()).getSchema());
			if (!queries.containsKey(qid)) {
				int numparts = (childrenAdded > degree) ? degree : 1;
				queries.put(
						qid,
						new SlaveQueryHandler(numparts, message.getQueryId(),
								queryHelper.getRewrittenQuery(), queryHelper
										.getResultSchema(), client));
			} else if (childrenAdded <= degree) {
				queries.get(qid).numPartsExpected++;
			}

			int temp = query.getPartition() % directChildren.size();
			directChildren.get(temp).write(message);
			break;
		case LEAF:
			try {
				String tableName = query.getTableName();
				Tablet tablet = dataStore.getTablet(tableName,
						query.getPartition());

				log.debug("Leaf processing Tablet: {}:{}", tablet.getSchema()
						.getName(), query.getPartition());

				QueryHelper helper = new QueryHelper(query, tablet.getSchema());
				QueryExecutor qx = new QueryExecutor(helper);

				qx.performQuery(tablet);
				qx.finalizeGroups();

				InMemoryWriteonlyTablet resultTablet = qx.getResultTablet();

				TabletMessage response = new TabletMessage(
						message.getQueryId(), resultTablet.serialize());
				handleResult(response);
			} catch (IOException e) {
				log.warn("Caught exception while creating result: {}",
						e.getMessage());
			}
			break;
		case STARTUP:
			log.warn("Query received during startup.");
			parentChannel.write(new SimpleMessage(MessageType.NACK, message
					.getQueryId(), new byte[] { (byte) 0 }));
			break;
		}
	}

	/**
	 * Adds a channel as direct child to the Slave.
	 * 
	 * @param channel
	 *            new child's Channel
	 * @param message
	 *            registration message
	 */
	private void addChild(Channel channel, SimpleMessage message) {
		// TODO adding children properly and efficiently
		ServingChannel newChild = new ServingChannel(channel,
				message.getPayload());

		log.info("Adding child: " + newChild);
		synchronized (directChildren) {
			if (childChannels.size() < degree) {
				// there is still room left for a direct child
				status = NodeStatus.INNER;
				directChildren.add(newChild);
			} else {
				// find a suitable parent
				int temp = childrenAdded % degree;
				ServingChannel ch = directChildren.get(temp);
				newChild.write(new SimpleMessage(MessageType.REDIR, -1, ch
						.getServingAddress().toString().getBytes()));
			}
			// add new child to ChannelGroup / List
			childChannels.add(channel);
			childrenAdded++;
		}
		log.debug("direct " + directChildren.size() + " / degree " + degree
				+ " / added " + childrenAdded);
	}

	/**
	 * @param payload
	 *            bytes of a representative String
	 * @return SocketAddress equivalent to the bytes provided
	 */
	private InetSocketAddress getSockAddrFromBytes(byte[] payload) {
		// TODO
		String temp = new String(payload);
		// log.debug(temp);
		String[] split = temp.split(":");
		String ipaddr = split[0].replaceAll("/", "");
		int port = Integer.parseInt(split[1]);
		// log.debug(ipaddr + " " + port);
		return new InetSocketAddress(ipaddr, port);
	}

	/**
	 * @return Message to register with a ChannelNode, the payload being the
	 *         port this listens for client connections
	 */
	private SimpleMessage getRegMsg() {

		return new SimpleMessage(MessageType.REGINNER, 0,
				ServingChannel.intToByte(ownPort));
	}

	@Override
	public boolean connectNReg(SocketAddress address) {
		// TODO
		log.info("Connecting to new parent node at {}", address);

		connecting = true;
		ChannelFactory factory = new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());

		// if (bootstrapParent != null) {
		// bootstrapParent.releaseExternalResources();
		// }
		if (parentChannel != null) {
			parentChannel.disconnect();
		}

		bootstrapParent = new ClientBootstrap(factory);
		bootstrapParent.setPipelineFactory(new GenericPipelineFac(this));
		bootstrapParent.setOption("connectTimeoutMillis", CONNECT_TIMEOUT);
		ChannelFuture connectFuture = bootstrapParent.connect(address);

		connectFuture.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {
				parentChannel = future.getChannel();
				openChannels.add(parentChannel);
				parentChannel.write(getRegMsg());
				connecting = false;
				log.info("Connected to parent at {}",
						parentChannel.getRemoteAddress());
			}
		});
		Runtime.getRuntime().addShutdownHook(new ShutdownStopper(this));

		return true;
	}

	/**
	 * Handles TabletMessages containing result data.
	 * 
	 * @param message
	 *            a TabletMessage containing a result tablet.
	 */
	private void handleResult(TabletMessage message) {
		// TODO aggregating of partial results within intermediate layer
		int qid = message.getQueryId();
		SlaveQueryHandler sqh = queries.get(qid);
		if (sqh != null) {
			if (sqh.numPartsExpected > 1) {
				sqh.addPart(message);
				log.debug("Received {}/{} parts.", sqh.parts.size(),
						sqh.numPartsExpected);
			} else {
				sqh.client.write(message);
				sqh.close();
			}
		} else {
			if (parentChannel != null && parentChannel.isConnected()) {
				parentChannel.write(message);
			} else if (coordinatorChannel != null
					&& coordinatorChannel.isConnected()) {
				coordinatorChannel.write(message);
			} else {
				log.warn("Received internal result w/o parent connection available.");
			}
		}
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		log.debug("Message received from {}.", e.getRemoteAddress());
		if (e.getMessage() instanceof QueryMessage) {
			log.debug("Message: {}", e.getMessage().toString());
			query((QueryMessage) e.getMessage());
		} else if (e.getMessage() instanceof TabletMessage) {
			log.debug("Message: {}", e.getMessage().toString());
			handleResult((TabletMessage) e.getMessage());
		} else if (e.getMessage() instanceof SimpleMessage) {
			SimpleMessage message = ((SimpleMessage) e.getMessage());
			log.debug("Message: {}", message.toString());
			switch (message.getType()) {
			case ACK:
				break;
			case REDIR:
				this.connectNReg(getSockAddrFromBytes(message.getPayload()));
				break;
			case INTERNAL_RESULT:
				if (parentChannel != null && parentChannel.isConnected()) {
					parentChannel.write(message);
				} else if (coordinatorChannel != null
						&& coordinatorChannel.isConnected()) {
					coordinatorChannel.write(message);
				} else {
					log.warn("Received internal result w/o parent connection available.");
				}
				break;
			case REGLEAF:
			case REGINNER:
				this.addChild(e.getChannel(), message);
				break;
			// case INTERNAL_QUERY:
			case CLIENT_QUERY:
			case CLIENT_RESULT:
			case REGCLIENT:
			case UNKNOWN:
				e.getChannel().write(
						new SimpleMessage(MessageType.NACK, message
								.getQueryID(), new byte[] { (byte) 0 }));
				break;

			}
		}
	}

	@Override
	public void removeChannel(Channel channel) {
		// TODO
		log.debug("Channel to {} closed.", channel.getRemoteAddress());
		if (coordinatorChannel == channel) {
			// TODO
			stop();
		} else if (parentChannel == channel) {
			// TODO reconnect to the coordinator to reenter the tree?
			if (!connecting) {
				// parentChannel = null;
				// if (coordinatorChannel.isConnected()) {
				// // coordinatorChannel.write(getRegMsg());
				// } else {
				// // connectNReg(serverHost, serverPort);
				// }
				// //
				// log.info("Connection to parent lost. Contacting Coordinator.");
			}
		} else {
			// connection to a child lost
			for (ServingChannel sc : directChildren) {
				if (sc.equals(channel)) {
					directChildren.remove(sc);
					if (directChildren.isEmpty()) {
						status = NodeStatus.LEAF;
					}
					break;
				}
			}
		}
		super.removeChannel(channel);
	}

	@Override
	public void stop() {
		// TODO proper shut down
		log.info("Stopping slave at {}.", coordinatorChannel);
		super.close();// should disconnect and close all open channels
		coordinatorChannel = null;
		directChildren.clear();
		childChannels.clear();

		if (bootstrapCoordinator != null) {
			bootstrapCoordinator.releaseExternalResources();
			bootstrapCoordinator = null;
		}
		if (bootstrapParent != null) {
			bootstrapParent.releaseExternalResources();
			bootstrapParent = null;
		}
		if (bootstrapServer != null) {
			bootstrapServer.releaseExternalResources();
			bootstrapServer = null;
		}
		log.info("Slave stopped.");
	}

	private static void printUsage() {
		System.out.println("Usage:");
		System.out.println("degree hostname port data-dir configFile");
	}

	public static void main(String[] args) throws InterruptedException {
		// Print usage if necessary.
		if (args.length < 5) {
			printUsage();
			return;
		}
		// Parse options.
		int degree = Integer.parseInt(args[0]);
		String host = args[1];
		int port = Integer.parseInt(args[2]);
		File dataDirectory = new File(args[3]);
		String conf = args[4];

		Slave slave = new Slave(degree, host, port, dataDirectory, conf);
		slave.start();
	}
}

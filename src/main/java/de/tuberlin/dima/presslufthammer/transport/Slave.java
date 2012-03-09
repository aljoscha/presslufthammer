package de.tuberlin.dima.presslufthammer.transport;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
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

import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryWriteonlyTablet;
import de.tuberlin.dima.presslufthammer.data.columnar.local.LocalDiskDataStore;
import de.tuberlin.dima.presslufthammer.qexec.QueryExecutor;
import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.transport.messages.MessageType;
import de.tuberlin.dima.presslufthammer.transport.messages.QueryMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.TabletMessage;
import de.tuberlin.dima.presslufthammer.transport.util.GenericPipelineFac;
import de.tuberlin.dima.presslufthammer.transport.util.ServingChannel;
import de.tuberlin.dima.presslufthammer.util.ShutdownStopper;
import de.tuberlin.dima.presslufthammer.util.Stoppable;

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
	 * 
	 */
	private LocalDiskDataStore dataStore;
	private boolean connecting = false;

	/**
	 * Constructor<br />
	 * Reads data from the data directory.
	 * 
	 * @param degree
	 *            the number of direct children the slave should accept
	 * @param serverHost
	 *            coordinator hostname or address
	 * @param serverPort
	 *            coordinator port
	 * @param dataDirectory
	 *            directory containing data sources
	 */
	public Slave(int degree, String serverHost, int serverPort,
			File dataDirectory) {
		if (degree < 1) {
			log.error("not a valid degree: " + degree, new Exception());
		}
		this.serverHost = serverHost;
		this.serverPort = serverPort;
		this.degree = degree;

		try {
			dataStore = LocalDiskDataStore.openDataStore(dataDirectory);
		} catch (IOException e) {
			log.warn("Exception caught while while loading datastore: {}",
					e.getMessage());
		}
	}

	/**
	 * Sets up a server socket listening for client connections. Connects to the
	 * coordinator using the connection info supplied to the constructor.
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
		// log.debug( s);
		String[] temp = s.split(":");

		return Integer.parseInt(temp[temp.length - 1]);
	}

	public void query(QueryMessage message) {
		Query query = message.getQuery();
		log.info("Received query: " + query);

		switch (status) {
		case INNER:
			// TODO rewriting of query
			int temp = query.getPartition() % directChildren.size();
			directChildren.get(temp).write(message);
			break;
		case LEAF:
			try {
				String tableName = query.getTableName();
				Tablet tablet = dataStore.getTablet(tableName,
						query.getPartition());

				log.debug("Tablet: {}:{}", tablet.getSchema().getName(),
						query.getPartition());

				QueryExecutor qx = new QueryExecutor(tablet, query);

				InMemoryWriteonlyTablet resultTablet = qx.performQuery();

				TabletMessage response = new TabletMessage(
						message.getQueryId(), resultTablet.serialize());
				parentChannel.write(response);
			} catch (IOException e) {
				log.warn("Caught exception while creating result: {}",
						e.getMessage());
			}
			break;
		case STARTUP:
			log.warn("Query received during startup");
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.dima.presslufthammer.transport.ChannelNode#connectNReg(java
	 * .net.SocketAddress)
	 */
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

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		log.debug("Message received from {}.", e.getRemoteAddress());
		if (e.getMessage() instanceof QueryMessage) {
			query((QueryMessage) e.getMessage());
		} else if (e.getMessage() instanceof TabletMessage) {
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
				// TODO aggregating of partial results within intermediate layer
				if (parentChannel != null && parentChannel.isConnected()) {
					parentChannel.write(message);
				} else if (coordinatorChannel != null
						&& coordinatorChannel.isConnected()) {
					coordinatorChannel.write(message);
				} else {
					log.warn("Received internal result w/o parent connection available.");
				}
				break;
			case REGINNER:
				this.addChild(e.getChannel(), message);
				break;
			case INTERNAL_QUERY:
			case REGLEAF:
			case UNKNOWN:
			case CLIENT_QUERY:
			case CLIENT_RESULT:
			case REGCLIENT:
				e.getChannel().write(
						new SimpleMessage(MessageType.NACK, message
								.getQueryID(), new byte[] { (byte) 0 }));
				break;

			}
		}
	}

	private void handleResult(TabletMessage message) {
		// TODO aggregating of partial results within intermediate layer
		if (parentChannel != null && parentChannel.isConnected()) {
			parentChannel.write(message);
		} else if (coordinatorChannel != null
				&& coordinatorChannel.isConnected()) {
			coordinatorChannel.write(message);
		} else {
			log.warn("Received internal result w/o parent connection available.");
		}
	}

	@Override
	public void removeChannel(Channel channel) {
		// TODO
		log.debug("Channel to {} closed.", channel.getRemoteAddress());
		if (parentChannel == channel) {
			if (!connecting) {
				parentChannel = null;
				if (coordinatorChannel.isConnected()) {
					coordinatorChannel.write(getRegMsg());
					System.out.println("AAA");
				} else {
					connectNReg(serverHost, serverPort);
					System.out.println("BBB");
				}
				log.info("Connection to parent lost. Contacting Coordinator.");
			}
		} else {
			for (ServingChannel sc : directChildren) {
				if (sc.equals(channel)) {
					directChildren.remove(sc);
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
	//
	// private static void printUsage() {
	// System.out.println("Usage:");
	// System.out.println("degree hostname port data-dir");
	// }
	//
	// public static void main(String[] args) throws InterruptedException {
	// // Print usage if necessary.
	// if (args.length < 4) {
	// printUsage();
	// return;
	// }
	// // Parse options.
	// int degree = Integer.parseInt(args[0]);
	// String host = args[1];
	// int port = Integer.parseInt(args[2]);
	// File dataDirectory = new File(args[3]);
	//
	// Slave slave = new Slave(degree, host, port, dataDirectory);
	// slave.start();
	// }
}

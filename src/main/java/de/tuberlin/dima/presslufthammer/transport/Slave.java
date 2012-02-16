package de.tuberlin.dima.presslufthammer.transport;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

import com.google.common.collect.Sets;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.Tablet;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryWriteonlyTablet;
import de.tuberlin.dima.presslufthammer.data.columnar.ondisk.OnDiskDataStore;
import de.tuberlin.dima.presslufthammer.qexec.TabletProjector;
import de.tuberlin.dima.presslufthammer.query.Projection;
import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.Type;
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

	private static int CONNECT_TIMEOUT = 10000;
	private final Logger log = LoggerFactory.getLogger(getClass());

	private Channel coordinatorChannel;
	private Channel parentChannel;
	private Channel servingChannel;
	private ClientBootstrap bootstrap;
	private ClientBootstrap bootstrapParent;
	private ServerBootstrap serverBootstrap;

	private String serverHost;
	private int serverPort;
	/**
	 * port this slave is listening for clients (children) on
	 */
	private int ownPort;
	/**
	 * number of direct children the slave accepts
	 */
	private final int degree;
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

	private OnDiskDataStore dataStore;

	/**
	 * Constructor<br />
	 * Reads data from the data directory.
	 * 
	 * @param serverHost
	 *            coordinator hostname or address
	 * @param serverPort
	 *            coordinator port
	 * @param dataDirectory
	 *            directory containing data sources
	 */
	public Slave(String serverHost, int serverPort, File dataDirectory) {
		this.serverHost = serverHost;
		this.serverPort = serverPort;
		this.degree = 2;

		try {
			dataStore = OnDiskDataStore.openDataStore(dataDirectory);
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
		bootstrap = new ClientBootstrap(factory);

		bootstrap.setPipelineFactory(new GenericPipelineFac(this));
		bootstrap.setOption("connectTimeoutMillis", CONNECT_TIMEOUT);

		SocketAddress address = new InetSocketAddress(serverHost, serverPort);

		ChannelFuture connectFuture = bootstrap.connect(address);
		// use a listener because awaitUninter... fails on multiconn/threaded
		// stuff
		connectFuture.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {
				coordinatorChannel = future.getChannel();
				parentChannel = coordinatorChannel;
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
		serverBootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		// Set up the event pipeline factory.
		serverBootstrap.setPipelineFactory(new GenericPipelineFac(this));

		// Bind and start to accept incoming connections.
		try {
			servingChannel = serverBootstrap
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

	@Override
	public void query(SimpleMessage message) {
		Query query = new Query(message.getPayload());
		log.info("Received query: " + query);

		String table = query.getFrom();

		switch (status) {
		case INNER:
			// TODO rewriting of query
			int temp = query.getPart() % directChildren.size();
			directChildren.get(temp).write(message);
			break;
		case LEAF:
			try {
				Tablet tablet = dataStore.getTablet(table, query.getPart());
				log.debug("Tablet: {}:{}", tablet.getSchema().getName(),
						query.getPart());

				Set<String> projectedFields = Sets.newHashSet();
				for (Projection project : query.getSelect()) {
					projectedFields.add(project.getColumn());
				}
				SchemaNode projectedSchema = null;
				if (projectedFields.contains("*")) {
					log.debug("Query is a 'select * ...' query.");
					projectedSchema = tablet.getSchema();
				} else {

					projectedSchema = tablet.getSchema().project(
							projectedFields);
				}

				InMemoryWriteonlyTablet resultTablet = new InMemoryWriteonlyTablet(
						projectedSchema);

				TabletProjector copier = new TabletProjector();
				copier.project(projectedSchema, tablet, resultTablet);

				SimpleMessage response = new SimpleMessage(
						Type.INTERNAL_RESULT, message.getQueryID(),
						resultTablet.serialize());

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

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		log.debug("Message received from {}.", e.getRemoteAddress());
		if (e.getMessage() instanceof SimpleMessage) {
			SimpleMessage message = ((SimpleMessage) e.getMessage());
			log.debug("Message: {}", message.toString());
			switch (message.getType()) {
			case ACK:
				break;
			case REDIR:
				this.connectNReg(getSockAddrFromBytes(message.getPayload()));
				break;
			case INTERNAL_QUERY:
				this.query(message);
				break;
			case INTERNAL_RESULT:
				// TODO do something
				parentChannel.write(message);
				break;
			case REGINNER:
				this.addChild(e.getChannel(), message);
				break;
			case REGLEAF:
			case UNKNOWN:
			case CLIENT_QUERY:
			case CLIENT_RESULT:
			case REGCLIENT:
				break;

			}
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
		// the new Child channel will be a direct descendant
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
				newChild.write(new SimpleMessage(Type.REDIR, (byte) -1, ch
						.getServingAddress().toString().getBytes()));
			}
			// add new child to ChannelGroup / List
			childChannels.add(channel);
			childrenAdded++;
		}
		log.debug("direct " + directChildren.size() + " / degree " + degree
				+ " / added " + childrenAdded);
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
				log.info("Connected to parent at {}",
						parentChannel.getRemoteAddress());
			}
		});
		Runtime.getRuntime().addShutdownHook(new ShutdownStopper(this));

		return true;
	}

	/**
	 * @return Message to register with a ChannelNode, the payload being the
	 *         port this listens for client connections
	 */
	private SimpleMessage getRegMsg() {

		return new SimpleMessage(Type.REGINNER, (byte) 0,
				ServingChannel.intToByte(ownPort));
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

	@Override
	public void stop() {
		// TODO proper shut down
		log.info("Stopping slave at {}.", coordinatorChannel);
		super.close();
		// if (coordinatorChannel != null) {
		// if( coordinatorChannel.isConnected()) {
		// coordinatorChannel.disconnect().awaitUninterruptibly();
		// }
		// coordinatorChannel.close();
		// coordinatorChannel = null;
		// }
		// if( childChannels != null) {
		// childChannels.close();
		// }
		if (bootstrap != null) {
			bootstrap.releaseExternalResources();
			bootstrap = null;
		}
		if (bootstrapParent != null) {
			bootstrapParent.releaseExternalResources();
			bootstrapParent = null;
		}
		if (serverBootstrap != null) {
			serverBootstrap.releaseExternalResources();
			serverBootstrap = null;
		}
		log.info("Slave stopped.");
	}
	//
	// private static void printUsage() {
	// System.out.println("Usage:");
	// System.out.println("hostname port data-dir");
	// }
	//
	// public static void main(String[] args) throws InterruptedException {
	// // Print usage if necessary.
	// if (args.length < 3) {
	// printUsage();
	// return;
	// }
	// // Parse options.
	// String host = args[0];
	// int port = Integer.parseInt(args[1]);
	// File dataDirectory = new File(args[2]);
	//
	// Slave leaf = new Slave(host, port, dataDirectory);
	// leaf.start();
	// }
}

package de.tuberlin.dima.presslufthammer.transport;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
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
	private int ownPort;
	private final int degree;
	private int childrenAdded = 0;

	private List<ServingChannel> directChildren = new ArrayList<ServingChannel>();
	private NodeStatus status = NodeStatus.STARTUP;

	private OnDiskDataStore dataStore;
	private ChannelGroup childChannels = new DefaultChannelGroup();

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

		connectFuture.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {
				coordinatorChannel = future.getChannel();
				// ownPort = getPortFromSocketAddress(coordinatorChannel
				// .getLocalAddress()) + 1;
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
			for (ServingChannel c : directChildren) {
				log.debug("querying: " + c.getRemoteAddress());
				c.write(message);
			}
			break;
		case LEAF:
			// log.debug("Query processing disabled for debugging purposes");
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

				coordinatorChannel.write(response);
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
			SimpleMessage simpleMsg = ((SimpleMessage) e.getMessage());
			log.debug("Message: {}", simpleMsg.toString());
			switch (simpleMsg.getType()) {
			case ACK:
				break;
			case REDIR:
				this.connectNReg(getSockAddrFromBytes(simpleMsg.getPayload()));
				break;
			case INTERNAL_QUERY:
				this.query(simpleMsg);
				break;
			case REGINNER:
				this.addChild(e.getChannel(), simpleMsg);
				break;
			case REGLEAF:
			case INTERNAL_RESULT:
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
	 * @param simpleMsg
	 *            registration message
	 */
	private void addChild(Channel channel, SimpleMessage simpleMsg) {
		// TODO adding children properly and efficiently
		// the new Child channel will be a direct descendant
		ServingChannel newChild = new ServingChannel(channel,
				simpleMsg.getPayload());

		log.info("Adding child: " + newChild);
		synchronized (directChildren) {
			if (childChannels.size() < degree) {
				// there is still room left for a direct child
				status = NodeStatus.INNER;
			} else {
				// the new child has to replace one of the existing children
				// and become it's new parent
				// so we find the parent that should be replaced
				int temp = childrenAdded % degree;
				ServingChannel ch = directChildren.get(temp);
				directChildren.remove(temp);
				Channel tempChan = ch.getChannel();
				// and tell it to connect to the new child
				tempChan.write(new SimpleMessage(Type.REDIR, (byte) -1,
						newChild.getServingAddress().toString().getBytes()));

			}
			// add new child to ChannelGroup / List
			childChannels.add(channel);
			directChildren.add(newChild);
			childrenAdded++;
		}
		log.debug("direct " + directChildren.size() + " / degree " + degree
				+ " / all " + childrenAdded);
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
		if (coordinatorChannel != null) {
			coordinatorChannel.close().awaitUninterruptibly();
		}
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

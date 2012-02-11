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
 * @author feichh
 * @author Aljoscha Krettek
 * 
 */
public class Slave extends ChannelNode implements Stoppable {

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
		//
		// try {
		// dataStore = OnDiskDataStore.openDataStore(dataDirectory);
		// } catch (IOException e) {
		// log.warn("Exception caught while while loading datastore: {}",
		// e.getMessage());
		// }
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

		// if (connectFuture.awaitUninterruptibly().isSuccess()) {
		// coordinatorChannel = connectFuture.getChannel();
		// parentChannel = coordinatorChannel;
		// coordinatorChannel.write(REGMSG);
		// log.info("Connected to coordinator at {}:{}", serverHost,
		// serverPort);
		// Runtime.getRuntime().addShutdownHook(new ShutdownStopper(this));
		// } else {
		// bootstrap.releaseExternalResources();
		// log.info("Failed to conncet to coordinator at {}:{}", serverHost,
		// serverPort);
		// }
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
	 * 
	 */
	public void serve() {
		// Configure the server.
		ServerBootstrap bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		// Set up the event pipeline factory.
		bootstrap.setPipelineFactory(new GenericPipelineFac(this));

		// Bind and start to accept incoming connections.
		boolean unbound = true;
		try {
			servingChannel = bootstrap.bind(new InetSocketAddress(ownPort));
			ownPort = getPortFromSocketAddress(servingChannel.getLocalAddress());
			unbound = false;
			log.info("serving on port: " + ownPort);
		} catch (org.jboss.netty.channel.ChannelException e) {
			log.warn("failed to bind at " + ownPort);
		}
	}

	/**
	 * @param localAddress
	 * @return
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
			for (Channel c : childChannels) {
				log.debug("querying: " + c.getRemoteAddress());
				c.write(message);
			}
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

				coordinatorChannel.write(response);
			} catch (IOException e) {
				log.warn("Caught exception while creating result: {}",
						e.getMessage());
			}
			break;
		case STARTUP:
			break;
		}
	}

	public void query(Query query) {
		// TODO
		log.debug("Query received: " + query);
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
	 * @param channel
	 * @param simpleMsg
	 */
	private void addChild(Channel channel, SimpleMessage simpleMsg) {
		// TODO
		log.info("adding child: " + channel);
		if (childChannels.size() < degree) {
			status = NodeStatus.INNER;
		} else {
			int temp = childrenAdded % degree;
			Iterator<Channel> it = childChannels.iterator();
			Channel tempChan = null;
			int i = 0;
			while (i <= temp && it.hasNext()) {
				tempChan = it.next();
				i++;
			}
			if (tempChan != null) {
				tempChan.write(new SimpleMessage(Type.REDIR, (byte) -1, channel
						.getRemoteAddress().toString().getBytes()));
			} else {
				log.warn("could not find parent for {}", channel);
			}
		}
		childChannels.add(channel);
		directChildren.add(new ServingChannel(channel, simpleMsg.getPayload()));
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
		log.info("connecting to new parent node at {}", address);

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

		// if (connectFuture.awaitUninterruptibly().isSuccess()) {
		// parentChannel = connectFuture.getChannel();
		// parentChannel.write(REGMSG);
		// log.info("Connected to {}", address);
		// Runtime.getRuntime().addShutdownHook(new ShutdownStopper(this));
		// } else {
		// bootstrapParent.releaseExternalResources();
		// log.info("Failed to conncet {}", address);
		// }
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

	private SimpleMessage getRegMsg() {

		return new SimpleMessage(Type.REGINNER, (byte) 0,
				ServingChannel.intToByte(ownPort));
		// return new SimpleMessage(Type.REGINNER, (byte) 0,
		// ("" + ownPort).getBytes());
	}

	/**
	 * @param payload
	 * @return
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
		log.info("Stopping leaf.");
		if (coordinatorChannel != null) {
			coordinatorChannel.close().awaitUninterruptibly();
		}
		bootstrap.releaseExternalResources();
		log.info("Leaf stopped.");
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

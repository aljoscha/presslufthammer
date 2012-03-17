/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

import java.io.ByteArrayOutputStream;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.transport.messages.TabletMessage;

/**
 * @author h
 * 
 */
public class SlaveQueryHandler extends QueryHandler {
	
    private Logger log = LoggerFactory.getLogger(getClass());

	public SlaveQueryHandler(long parts, int queryId, Query resultQuery,
			SchemaNode schema, Channel client) {
		super(parts, queryId, resultQuery, schema, client);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.dima.presslufthammer.transport.QueryHandler#sendResult(java
	 * .io.ByteArrayOutputStream)
	 */
	@Override
	protected void sendResult(ByteArrayOutputStream outArray) {
		if (client != null) {
			if (outArray.size() < 1) {
				log.warn("Assembled response has size {}", outArray.size());
			}
			client.write(new TabletMessage(queryID, outArray.toByteArray()));
		} else {
			log.warn("No client in QueryHandler.");
		}
	}

}

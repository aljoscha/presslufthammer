/**
 * 
 */
package de.tuberlin.dima.presslufthammer.transport;

import java.io.IOException;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryReadonlyTablet;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryWriteonlyTablet;
import de.tuberlin.dima.presslufthammer.qexec.QueryExecutor;
import de.tuberlin.dima.presslufthammer.qexec.QueryHelper;
import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.transport.messages.TabletMessage;

/**
 * @author feihh
 * 
 */
public class SlaveQueryHandler extends QueryHandler {
	
    private Logger log = LoggerFactory.getLogger(getClass());

	public SlaveQueryHandler(long parts, int queryId, Query resultQuery,
			SchemaNode schema, Channel client) {
		super(parts, queryId, resultQuery, schema, client);
	}

	@Override
    protected void assemble() {
        QueryHelper helper = new QueryHelper(resultQuery, schema);
        QueryExecutor qx = new QueryExecutor(helper);

        try {
            for (InMemoryReadonlyTablet part : parts) {
                qx.performQuery(part);
            }
            qx.finalizeGroups();
        } catch (IOException e) {
            log.warn("Caught exception while assembling result for client: {}",
                    e.getMessage());
        }

        InMemoryWriteonlyTablet resultTablet = qx.getResultTablet();

		if (client != null) {
			
			client.write(new TabletMessage(queryID, resultTablet.serialize()));
		} else {
			log.warn("No client in QueryHandler.");
		}
    }
}

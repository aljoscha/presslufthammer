package de.tuberlin.dima.presslufthammer.transport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import de.tuberlin.dima.presslufthammer.data.AssemblyFSM;
import de.tuberlin.dima.presslufthammer.data.SchemaNode;
import de.tuberlin.dima.presslufthammer.data.columnar.inmemory.InMemoryReadonlyTablet;
import de.tuberlin.dima.presslufthammer.data.hierarchical.json.JSONRecordPrinter;
import de.tuberlin.dima.presslufthammer.query.Query;
import de.tuberlin.dima.presslufthammer.transport.messages.SimpleMessage;
import de.tuberlin.dima.presslufthammer.transport.messages.Type;

/**
 * @author feichh
 * @author Aljoscha Krettek
 * 
 */
public class QueryHandler {
    
	public enum QueryStatus {
		OPEN, CLOSED
	}
	
	private Logger log = LoggerFactory.getLogger(getClass());

	final byte queryID;
	final Channel client;
	final SimpleMessage queryMsg;
	final Query query;
	private int numPartsExpected;
	QueryStatus status;
	SchemaNode schema;
	List<InMemoryReadonlyTablet> parts;

	public QueryHandler(int parts, SimpleMessage queryMsg, SchemaNode schema, Channel client) {
		assert (queryMsg.getQueryID() > 0);
		this.parts = Lists.newLinkedList();
		this.numPartsExpected = parts;
		this.queryMsg = queryMsg;
		this.query = null;
		this.queryID = queryMsg.getQueryID();
		this.client = client;
		this.status = QueryStatus.OPEN;
		this.schema = schema;
	}

	public void addPart(SimpleMessage message) {
		if (message.getQueryID() == queryID && parts.size() < numPartsExpected) {
			parts.add(new InMemoryReadonlyTablet(message.getPayload()));
			if (parts.size() == numPartsExpected) {
				assemble();
			}
		}
	}

	private void assemble() {
	    ByteArrayOutputStream outArray = new ByteArrayOutputStream();
	    PrintWriter writer = new PrintWriter(outArray);
	    JSONRecordPrinter recordPrinter = new JSONRecordPrinter(schema, writer);
	    
	    AssemblyFSM assemblyFSM = new AssemblyFSM(schema);
	    
	    log.info("Assembling client response from {} tablets.", parts.size());
	    
	    for (InMemoryReadonlyTablet tablet : parts) {
	        try {
                assemblyFSM.assembleRecords(tablet, recordPrinter);
            } catch (IOException e) {
                log.warn("Caught exception while assembling result for client: {}", e.getMessage());
            }
	    }
	    writer.flush();
	    
		if (client != null) {
			client.write(new SimpleMessage(Type.CLIENT_RESULT, queryID, outArray.toByteArray()));
		} else {
		    log.warn("No client in QueryHandler.");
		}
		close();
	}

	public boolean isComplete() {
		return this.status == QueryStatus.CLOSED;
	}

	private void close() {
		status = QueryStatus.CLOSED;
	}
}
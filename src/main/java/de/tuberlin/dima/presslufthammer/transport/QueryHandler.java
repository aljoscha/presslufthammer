package de.tuberlin.dima.presslufthammer.transport;

import org.jboss.netty.channel.Channel;

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

	final byte queryID;
	final Channel client;
	final SimpleMessage queryMSG;
	final Query query;
	final int parts;
	private int parts_received;
	QueryStatus status;
	byte[][] data;

	public QueryHandler(int parts, SimpleMessage query, Channel client) {
		assert (query.getQueryID() > 0);
		this.parts = parts;
		this.queryMSG = query;
		this.query = null;
		this.queryID = query.getQueryID();
		this.client = client;
		this.data = new byte[parts][];
		this.status = QueryStatus.OPEN;
	}
	
	public QueryHandler(int parts, Query query, Channel client) {
		assert (query.getId() > 0);
		this.parts = parts;
		this.queryMSG = null;
		this.query = query;
		this.queryID = query.getId();
		this.client = client;
		this.data = new byte[parts][];
		this.status = QueryStatus.OPEN;
	}

	public void addPart(SimpleMessage partMSG) {
		// TODO
		if (partMSG.getQueryID() == queryID && parts > parts_received) {
			data[parts_received++] = partMSG.getPayload();
			if (parts == parts_received) {
				assemble();
			}
		}
	}

	private void assemble() {
		// TODO
		close();
		String r = getResult();
		if (client != null) {
			client.write(new SimpleMessage(Type.RESULT, queryID, r.getBytes()));
		} else {
			System.out.println(r);
		}
	}

	public String getResult() {
		String result = "Query result: ";
		if (isComplete()) {
			for (byte[] b : data) {
				result += new String(b) + " ";
			}
		} else {
			result = "not complete yet";
		}
		return result;
	}

	public boolean isComplete() {
		return this.status == QueryStatus.CLOSED;
	}

	private void close() {
		status = QueryStatus.CLOSED;
	}
}
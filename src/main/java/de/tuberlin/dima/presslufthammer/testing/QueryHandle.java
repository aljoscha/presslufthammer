/**
 * 
 */
package de.tuberlin.dima.presslufthammer.testing;

import org.jboss.netty.channel.Channel;

import de.tuberlin.dima.presslufthammer.pressluft.Pressluft;
import de.tuberlin.dima.presslufthammer.testing.Coordinator.QueryStatus;

/**
 * @author feichh
 *
 */
public class QueryHandle {

	final byte queryID;
	final Channel client;
	final Pressluft query;
	final int parts;
	private int parts_received;
	QueryStatus status;
	byte[][] data;

	public QueryHandle(int parts,Pressluft query, Channel client) {
		assert(query.getQueryID() > 0);
		this.parts = parts;
		this.query = query;
		this.queryID = query.getQueryID();
		this.client = client;
		this.data = new byte[parts][];
		this.status = QueryStatus.OPEN;
	}
	
	public void addPart(Pressluft partMSG) {
		// TODO
		if(partMSG.getQueryID() == queryID && parts > parts_received) {
			data[parts_received++] = partMSG.getPayload();
			if( parts == parts_received) {
				assemble();
			}
		}
	}
	
	private void assemble() {
		// TODO
		close();
		System.out.println( getResult());
	}

	public String getResult() {
		String result = "";
		if(isComplete()) {
			for( byte[] b: data) {
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
/**
 * 
 */
package de.tuberlin.dima.presslufthammer.pressluft;

import java.io.Serializable;

/**
 * based on: https://github.com/brunodecarvalho/netty-tutorials/
 * 
 */
@SuppressWarnings("serial")
public class Pressluft implements Serializable {
	private Type type;
	private byte queryID;
	private byte[] payload;

	/**
	 * 
	 */
	public Pressluft() {
	}

	/**
	 * @param t
	 * @param qid
	 * @param load
	 */
	public Pressluft(Type t, byte qid, byte[] load) {
		this.type = t;
		this.queryID = qid;
		this.payload = load;
	}

	/**
	 * @return the type
	 */
	public Type getType() {
		return type;
	}

	/**
	 * @param type
	 *            the type to set
	 */
	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * @return the payload
	 */
	public byte[] getPayload() {
		return payload;
	}

	/**
	 * @param payload
	 *            the payload to set
	 */
	public void setPayload(byte[] payload) {
		this.payload = payload;
	}

	/**
	 * @return
	 */
	public byte getQueryID() {
		return queryID;
	}

	/**
	 * @param queryID
	 */
	public void setQueryID(byte queryID) {
		this.queryID = queryID;
	}

	/**
	 * @param query
	 * @return
	 */
	public static Pressluft createQueryMSG(byte qid, String query) {
		// TODO
		Type type = Type.QUERY;
		byte[] payload = query.getBytes();

		return new Pressluft(type, qid, payload);
	}
	
	public static Pressluft createQueryMSG(String query) {
		Type type = Type.QUERY;
		byte[] payload = query.getBytes();

		return new Pressluft(type, (byte) 0, payload);
	}

	@Override
	public String toString() {
		return new StringBuilder().append("Pressluft{").append(" type=")
				.append(type).append("qid=").append(queryID).append(", payload=")
				.append(payload == null ? null : payload.length + "bytes")
				.append('}').toString();
	}
}

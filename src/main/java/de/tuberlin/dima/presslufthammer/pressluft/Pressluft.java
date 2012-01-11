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
	private byte[] payload;

	public Pressluft() {
	}

	public Pressluft(Type t, byte[] load) {
		this.type = t;
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
	 * @param query
	 * @return
	 */
	public static Pressluft getQueryMSG(String query) {
		// TODO
		Type type = Type.QUERY;
		byte[] payload = query.getBytes();

		return new Pressluft(type, payload);
	}

	@Override
	public String toString() {
		return new StringBuilder().append("Pressluft{").append(" type=")
				.append(type).append(", payload=")
				.append(payload == null ? null : payload.length + "bytes")
				.append('}').toString();
	}
}

package de.tuberlin.dima.presslufthammer.transport.messages;


public class SimpleMessage {
	private Type type;
	private byte queryID;
	private byte[] payload;

	public SimpleMessage() {
	}

	public SimpleMessage(Type t, byte qid, byte[] load) {
		this.type = t;
		this.queryID = qid;
		this.payload = load;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public byte[] getPayload() {
		return payload;
	}

	public void setPayload(byte[] payload) {
		this.payload = payload;
	}

	public byte getQueryID() {
		return queryID;
	}

	public void setQueryID(byte queryID) {
		this.queryID = queryID;
	}

	public static SimpleMessage getQueryMSG(byte qid, String query) {
		// TODO
		Type type = Type.INTERNAL_QUERY;
		byte[] payload = query.getBytes();

		return new SimpleMessage(type, qid, payload);
	}
	
	public static SimpleMessage getQueryMSG(String query) {
		Type type = Type.INTERNAL_QUERY;
		byte[] payload = query.getBytes();

		return new SimpleMessage(type, (byte) 0, payload);
	}

	@Override
	public String toString() {
	    StringBuilder result = new StringBuilder();
	    result.append("SimpleMessage{");
	    result.append("type=" + type);
	    result.append(", ");
	    result.append("qid=" + queryID);
	    result.append(", ");
	    result.append("payload=" + (payload == null ? null : payload.length + " bytes"));
	    result.append("}");
	    return result.toString();
	}
}

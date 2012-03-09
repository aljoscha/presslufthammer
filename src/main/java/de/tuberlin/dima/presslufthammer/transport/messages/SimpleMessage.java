package de.tuberlin.dima.presslufthammer.transport.messages;


/**
 * @author feichh
 * @author Aljoscha Krettek
 *
 */
public class SimpleMessage {
	private MessageType type;
	private int queryID;
	private byte[] payload;

	public SimpleMessage() {
	}

	public SimpleMessage(MessageType t, int qid, byte[] load) {
		this.type = t;
		this.queryID = qid;
		this.payload = load;
	}

	public MessageType getType() {
		return type;
	}

	public void setType(MessageType type) {
		this.type = type;
	}

	public byte[] getPayload() {
		return payload;
	}

	public void setPayload(byte[] payload) {
		this.payload = payload;
	}

	public int getQueryID() {
		return queryID;
	}

	public void setQueryID(byte queryID) {
		this.queryID = queryID;
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

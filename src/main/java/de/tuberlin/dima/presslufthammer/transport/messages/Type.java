package de.tuberlin.dima.presslufthammer.transport.messages;

/**
 * 
 * @author Aljoscha Krettek
 * @author feich
 * 
 */
public enum Type {
	INTERNAL_QUERY((byte) 0x01), // This used when communicating between coordinator and leafs
	CLIENT_QUERY((byte) 0x02), // This is send from client to coordinator
	INTERNAL_RESULT((byte) 0x03), // Again, leafs to coordinator (contains a tablet)
	CLIENT_RESULT((byte) 0x04), // Result in record from to client
	REGINNER((byte) 0x05),
	REGLEAF((byte) 0x06),
	REDIR((byte) 0x07),
	REGCLIENT((byte) 0x08),
//	OFFER((byte) 0x09),
//	ACCEPT((byte) 0x0A),
	ACK((byte) 0x09),
	// put last since it's the least likely one to be encountered in the
	// fromByte() function
	UNKNOWN((byte) 0x00);

	private final byte b;

	private Type(byte b) {
		this.b = b;
	}

	public static Type fromByte(byte b) {
		for (Type code : values()) {
			if (code.b == b) {
				return code;
			}
		}

		return UNKNOWN;
	}

	public byte getByteValue() {
		return b;
	}
}

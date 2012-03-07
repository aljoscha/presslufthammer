package de.tuberlin.dima.presslufthammer.transport.messages;

/**
 * The type of a message. For sending the message over the network
 * (encoding/decoding).
 * 
 * @author Aljoscha Krettek
 */
public enum MessageType {
    INTERNAL_QUERY((byte) 0x01), // This used when communicating between
                                 // coordinator and leafs
    CLIENT_QUERY((byte) 0x02), // This is send from client to coordinator
    INTERNAL_RESULT((byte) 0x03), // Again, leafs to coordinator (contains a
                                  // tablet)
    CLIENT_RESULT((byte) 0x04), // Result in record from to client
    REGINNER((byte) 0x05), REGLEAF((byte) 0x06), INFO((byte) 0x07), REGCLIENT(
            (byte) 0x08), ACK((byte) 0x09),
    // put last since it's the least likely one to be encountered in the
    // fromByte() function
    QUERY((byte) 0xA), TABLET((byte) 0xB), UNKNOWN((byte) 0x00);

    private final byte b;

    private MessageType(byte b) {
        this.b = b;
    }

    public static MessageType fromByte(byte b) {
        for (MessageType code : values()) {
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

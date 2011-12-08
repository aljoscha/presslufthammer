/**
 * 
 */
package de.tuberlin.dima.presslufthammer.pressluft;

/**
 * 
 * based on
 * https://github.com/brunodecarvalho/netty-tutorials
 *
 */
public enum Type {

  // constants ------------------------------------------------------------------------------------------------------

	ACK((byte) 0x05),
  QUERY((byte) 0x01),
  RESULT((byte) 0x02),
  REGINNER((byte) 0x03),
  REGLEAF((byte) 0x04),
  // put last since it's the least likely one to be encountered in the fromByte() function
  UNKNOWN((byte) 0x00);

  // internal vars --------------------------------------------------------------------------------------------------

  private final byte b;

  // constructors ---------------------------------------------------------------------------------------------------

  private Type(byte b) {
      this.b = b;
  }

  // public static methods ------------------------------------------------------------------------------------------

  public static Type fromByte(byte b) {
      for (Type code : values()) {
          if (code.b == b) {
              return code;
          }
      }

      return UNKNOWN;
  }

  // getters & setters ----------------------------------------------------------------------------------------------

  public byte getByteValue() {
      return b;
  }
}

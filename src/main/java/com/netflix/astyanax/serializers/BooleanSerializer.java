package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

/**
 * Converts bytes to Boolean and vice versa
 * 
 * @author Bozhidar Bozhanov
 * 
 */
public final class BooleanSerializer extends AbstractSerializer<Boolean> {

  private static final BooleanSerializer instance = new BooleanSerializer();

  public static BooleanSerializer get() {
    return instance;
  }

  @Override
  public ByteBuffer toByteBuffer(Boolean obj) {
    if (obj == null) {
      return null;
    }
    boolean bool = obj;
    byte[] b = new byte[1];
    b[0] = bool ? (byte) 1 : (byte) 0;

    return ByteBuffer.wrap(b);
  }

  @Override
  public Boolean fromByteBuffer(ByteBuffer bytes) {
    if ((bytes == null) || (bytes.remaining() < 1)) {
      return null;
    }
    byte b = bytes.get();
    return b == (byte) 1;
  }

}

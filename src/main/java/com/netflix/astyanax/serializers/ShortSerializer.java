package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

/**
 * {@link Serializer} for {@link Short}s (no pun intended).
 * 
 */
public final class ShortSerializer extends AbstractSerializer<Short> {

  private static final ShortSerializer instance = new ShortSerializer();

  public static ShortSerializer get() {
    return instance;
  }

  @Override
  public ByteBuffer toByteBuffer(Short obj) {
    if (obj == null) {
      return null;
    }
    ByteBuffer b = ByteBuffer.allocate(2);
    b.putShort(obj);
    b.rewind();
    return b;
  }

  @Override
  public Short fromByteBuffer(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }
    short in = byteBuffer.getShort();
    return in;
  }

}

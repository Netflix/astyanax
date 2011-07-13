package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

/**
 * Converts bytes to Integer and vice versa
 * 
 * @author Bozhidar Bozhanov
 * 
 */
public final class IntegerSerializer extends AbstractSerializer<Integer> {

  private static final IntegerSerializer instance = new IntegerSerializer();

  public static IntegerSerializer get() {
    return instance;
  }

  @Override
  public ByteBuffer toByteBuffer(Integer obj) {
    if (obj == null) {
      return null;
    }
    ByteBuffer b = ByteBuffer.allocate(4);
    b.putInt(obj);
    b.rewind();
    return b;
  }

  @Override
  public Integer fromByteBuffer(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }
    int in = byteBuffer.getInt();
    return in;
  }
  
  @Override
  public Integer fromBytes(byte[] bytes) {
    ByteBuffer bb = ByteBuffer.allocate(4).put(bytes, 0, 4);
    bb.rewind();
    return bb.getInt();
  }

}

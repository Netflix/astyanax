package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class PrimitiveIntArraySerializer extends AbstractSerializer<int[]> {

  private static final PrimitiveIntArraySerializer instance = new PrimitiveIntArraySerializer();

  public static PrimitiveIntArraySerializer get() {
    return instance;
  }

  @Override
  public ByteBuffer toByteBuffer(int[] obj) {
    if (null == obj) {
      return null;
    }
    ByteBuffer bb = ByteBuffer.allocate(obj.length * 4);
    bb.asIntBuffer().put(obj);
    bb.rewind();
    return bb;
  }

  @Override
  public int[] fromByteBuffer(ByteBuffer byteBuffer) {
    if (null == byteBuffer) {
      return null;
    }
    IntBuffer intBuffer = byteBuffer.duplicate().asIntBuffer();
    int[] ints = new int[intBuffer.remaining()];
    intBuffer.get(ints);
    return ints;
  }
}

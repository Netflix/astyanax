package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

public class PrimitiveDoubleArraySerializer extends AbstractSerializer<double[]> {

  private static final PrimitiveDoubleArraySerializer instance = new PrimitiveDoubleArraySerializer();

  public static PrimitiveDoubleArraySerializer get() {
    return instance;
  }

  @Override
  public ByteBuffer toByteBuffer(double[] obj) {
    if (null == obj) {
      return null;
    }
    ByteBuffer bb = ByteBuffer.allocate(obj.length * 8);
    bb.asDoubleBuffer().put(obj);
    bb.rewind();
    return bb;
  }

  @Override
  public double[] fromByteBuffer(ByteBuffer byteBuffer) {
    if (null == byteBuffer) {
      return null;
    }
    DoubleBuffer doubleBuffer = byteBuffer.duplicate().asDoubleBuffer();
    double[] doubles = new double[doubleBuffer.remaining()];
    doubleBuffer.get(doubles);
    return doubles;
  }
}

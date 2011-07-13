package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * A StringSerializer translates the byte[] to and from string using utf-8
 * encoding.
 * 
 * @author Ran Tavory
 * 
 */
public final class StringSerializer extends AbstractSerializer<String> {

  private static final String UTF_8 = "UTF-8";
  private static final StringSerializer instance = new StringSerializer();
  private static final Charset charset = Charset.forName(UTF_8);

  public static StringSerializer get() {
    return instance;
  }

  @Override
  public ByteBuffer toByteBuffer(String obj) {
    if (obj == null) {
      return null;
    }
    return ByteBuffer.wrap(obj.getBytes(charset));
  }

  @Override
  public String fromByteBuffer(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }
    return charset.decode(byteBuffer).toString();
  }

  @Override
  public ComparatorType getComparatorType() {
    return ComparatorType.UTF8TYPE;
  }

}

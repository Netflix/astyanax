package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.cassandra.db.marshal.AsciiType;

/**
 * Almost identical to StringSerializer except we use the US-ASCII character set
 * code
 * 
 * @author zznate
 */
public final class AsciiSerializer extends AbstractSerializer<String> {

  private static final String US_ASCII = "US-ASCII";
  private static final AsciiSerializer instance = new AsciiSerializer();
  private static final Charset charset = Charset.forName(US_ASCII);

  public static AsciiSerializer get() {
    return instance;
  }

  @Override
  public String fromByteBuffer(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }
    return charset.decode(byteBuffer).toString();
  }

  @Override
  public ByteBuffer toByteBuffer(String obj) {
    if (obj == null) {
      return null;
    }
    return ByteBuffer.wrap(obj.getBytes(charset));
  }

  @Override
  public ComparatorType getComparatorType() {
    return ComparatorType.ASCIITYPE;
  }

	@Override
	public ByteBuffer fromString(String str) {
	    return AsciiType.instance.fromString(str);
	}

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return AsciiType.instance.getString(byteBuffer);
    }
}

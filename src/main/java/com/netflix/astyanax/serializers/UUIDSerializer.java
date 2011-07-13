package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * A UUIDSerializer translates the byte[] to and from UUID types.
 * 
 * @author Ed Anuff
 * 
 */
public final class UUIDSerializer extends AbstractSerializer<UUID> {

  private static final UUIDSerializer instance = new UUIDSerializer();

  public static UUIDSerializer get() {
    return instance;
  }

  @Override
  public ByteBuffer toByteBuffer(UUID uuid) {
    if (uuid == null) {
      return null;
    }
    long msb = uuid.getMostSignificantBits();
    long lsb = uuid.getLeastSignificantBits();
    byte[] buffer = new byte[16];

    for (int i = 0; i < 8; i++) {
      buffer[i] = (byte) (msb >>> 8 * (7 - i));
    }
    for (int i = 8; i < 16; i++) {
      buffer[i] = (byte) (lsb >>> 8 * (7 - i));
    }

    return ByteBuffer.wrap(buffer);
  }

  @Override
  public UUID fromByteBuffer(ByteBuffer bytes) {
    if (bytes == null) {
      return null;
    }
    return new UUID(bytes.getLong(), bytes.getLong());
  }

  @Override
  public ComparatorType getComparatorType() {
    return ComparatorType.UUIDTYPE;
  }

}

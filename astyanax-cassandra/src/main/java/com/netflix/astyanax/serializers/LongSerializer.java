package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.LongType;

/**
 * Converts bytes to Long and vise a versa
 * 
 * @author Ran Tavory
 * 
 */
public final class LongSerializer extends AbstractSerializer<Long> {

    private static final LongSerializer instance = new LongSerializer();

    public static LongSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Long obj) {
        if (obj == null) {
            return null;
        }
        return ByteBuffer.allocate(8).putLong(0, obj);
    }

    @Override
    public Long fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null)
            return null;
        ByteBuffer dup = byteBuffer.duplicate();
        if (dup.remaining() == 8) {
            long l = dup.getLong();
            return l;
        } else if (dup.remaining() == 4) {
            return (long) dup.getInt();
        }
        return null;
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.LONGTYPE;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return LongType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return LongType.instance.getString(byteBuffer);
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        Long val = fromByteBuffer(byteBuffer.duplicate());
        if (val == Long.MAX_VALUE) {
            throw new ArithmeticException("Can't paginate past max long");
        }
        return toByteBuffer(val + 1);
    }
}

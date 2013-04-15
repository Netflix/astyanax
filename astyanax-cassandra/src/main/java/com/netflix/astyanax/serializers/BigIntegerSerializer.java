package com.netflix.astyanax.serializers;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.IntegerType;

/**
 * Serializer implementation for BigInteger
 * 
 * @author zznate
 */
public final class BigIntegerSerializer extends AbstractSerializer<BigInteger> {

    private static final BigIntegerSerializer INSTANCE = new BigIntegerSerializer();

    public static BigIntegerSerializer get() {
        return INSTANCE;
    }

    @Override
    public BigInteger fromByteBuffer(final ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        ByteBuffer dup = byteBuffer.duplicate();
        int length = dup.remaining();
        byte[] bytes = new byte[length];
        dup.get(bytes);
        return new BigInteger(bytes);
    }

    @Override
    public ByteBuffer toByteBuffer(BigInteger obj) {
        if (obj == null) {
            return null;
        }
        return ByteBuffer.wrap(obj.toByteArray());
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.INTEGERTYPE;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return IntegerType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return IntegerType.instance.getString(byteBuffer);
    }

    @Override
    public ByteBuffer getNext(final ByteBuffer byteBuffer) {
        if (byteBuffer == null)
            return null;
        BigInteger bigint = fromByteBuffer(byteBuffer.duplicate());
        bigint = bigint.add(new BigInteger("1"));
        return toByteBuffer(bigint);
    }

}

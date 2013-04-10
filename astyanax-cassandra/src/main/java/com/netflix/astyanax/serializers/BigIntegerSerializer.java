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
    public BigInteger fromByteBuffer(ByteBuffer byteBuffer) {
        try {
            if (byteBuffer == null) {
                return null;
            }
            int length = byteBuffer.remaining();
            byte[] bytes = new byte[length];
            byteBuffer.duplicate().get(bytes);
            return new BigInteger(bytes);
        } finally {
            if (byteBuffer != null)
                byteBuffer.rewind();
        }
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
        try {
            return IntegerType.instance.getString(byteBuffer);
        } finally {
            if (byteBuffer != null)
                byteBuffer.rewind();
        }
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        try {
            BigInteger bigint = fromByteBuffer(byteBuffer.duplicate());
            bigint = bigint.add(new BigInteger("1"));
            return toByteBuffer(bigint);
        } finally {
            if (byteBuffer != null)
                byteBuffer.rewind();
        }
    }

}

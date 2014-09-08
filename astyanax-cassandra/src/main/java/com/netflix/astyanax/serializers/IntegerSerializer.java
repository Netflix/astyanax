package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

/**
 * Converts bytes to Integer (32 bit) and vice versa.
 * 
 * It is important not to confuse IntegerSerializer with IntegerType in cassandra.
 * IntegerSerializer serializes 32 bit integers while IntegerType comparator in cassandra
 * is a BigInteger.
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
        if ((byteBuffer == null) || (byteBuffer.remaining() != 4)) {
            return null;
        }
        ByteBuffer dup = byteBuffer.duplicate();
        int in = dup.getInt();
        return in;
    }

    @Override
    public Integer fromBytes(byte[] bytes) {
        if ((bytes == null) || (bytes.length != 4)) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.allocate(4).put(bytes, 0, 4);
        bb.rewind();
        return bb.getInt();
    }

    @Override
    public ByteBuffer fromString(String str) {
        return toByteBuffer(Integer.parseInt(str));
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return Integer.toString(fromByteBuffer(byteBuffer));
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        Integer val = fromByteBuffer(byteBuffer.duplicate());
        if (val == Integer.MAX_VALUE) {
            throw new ArithmeticException("Can't paginate past max int");
        }
        return toByteBuffer(val + 1);
    }

    public ComparatorType getComparatorType() {
        return ComparatorType.INT32TYPE;
    }

}

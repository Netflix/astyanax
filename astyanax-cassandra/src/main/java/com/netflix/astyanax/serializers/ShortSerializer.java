package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.IntegerType;

/**
 * {@link Serializer} for {@link Short}s (no pun intended).
 * 
 */
public final class ShortSerializer extends AbstractSerializer<Short> {

    private static final ShortSerializer instance = new ShortSerializer();

    public static ShortSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Short obj) {
        if (obj == null) {
            return null;
        }
        ByteBuffer b = ByteBuffer.allocate(2);
        b.putShort(obj);
        b.rewind();
        return b;
    }

    @Override
    public Short fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        ByteBuffer dup = byteBuffer.duplicate();
        short in = dup.getShort();
        return in;
    }

    @Override
    public ByteBuffer fromString(String str) {
        // Verify value is a short
        return toByteBuffer(Short.parseShort(str));
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return IntegerType.instance.getString(byteBuffer);
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        Short val = fromByteBuffer(byteBuffer.duplicate());
        if (val == Short.MAX_VALUE) {
            throw new ArithmeticException("Can't paginate past max short");
        }
        val++;
        return toByteBuffer(val);
    }
}

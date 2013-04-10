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
        try {
            if (byteBuffer == null) {
                return null;
            }
            short in = byteBuffer.getShort();
            return in;
        } finally {
            if (byteBuffer != null)
                byteBuffer.rewind();
        }
    }

    @Override
    public ByteBuffer fromString(String str) {
        // Verify value is a short
        return toByteBuffer(Short.parseShort(str));
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
            Short val = fromByteBuffer(byteBuffer.duplicate());
            if (val == Short.MAX_VALUE) {
                throw new ArithmeticException("Can't paginate past max short");
            }
            val++;
            return toByteBuffer(val);
        } finally {
            if (byteBuffer != null)
                byteBuffer.rewind();
        }
    }
}

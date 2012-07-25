package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

/**
 * Converts bytes to Integer and vice versa
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
        int in = byteBuffer.getInt();
        byteBuffer.rewind();
        return in;
    }

    @Override
    public Integer fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length > 4) {
            return null;
        }
        else if (bytes.length < 4) {
            byte[] newBytes = new byte[4];

            for (int i = bytes.length - 1; i >= 0; i--) {
                newBytes[i + 4 - bytes.length] = bytes[i];
            }
            bytes = newBytes;
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

}

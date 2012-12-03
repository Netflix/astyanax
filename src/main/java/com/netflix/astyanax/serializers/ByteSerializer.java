package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

public class ByteSerializer extends AbstractSerializer<Byte> {

    private static final ByteSerializer instance = new ByteSerializer();

    public static ByteSerializer get() {
        return instance;
    }
    
    @Override
    public ByteBuffer toByteBuffer(Byte obj) {
        if (obj == null) {
            return null;
        }
        ByteBuffer b = ByteBuffer.allocate(1);
        b.put(obj);
        b.rewind();
        return b;
    }

    @Override
    public Byte fromByteBuffer(ByteBuffer byteBuffer) {
        if ((byteBuffer == null) || (byteBuffer.remaining() != 4)) {
            return null;
        }
        byte by = byteBuffer.get();
        byteBuffer.rewind();
        return by;
    }

    @Override
    public Byte fromBytes(byte[] bytes) {
        if ((bytes == null) || (bytes.length != 1)) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.allocate(1).put(bytes, 0, 1);
        bb.rewind();
        return bb.get();
    }

    @Override
    public ByteBuffer fromString(String str) {
        return toByteBuffer(Byte.parseByte(str));
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return Byte.toString(fromByteBuffer(byteBuffer));
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        Byte val = fromByteBuffer(byteBuffer.duplicate());
        if (val == Byte.MAX_VALUE) {
            throw new ArithmeticException("Can't paginate past max byte");
        }
        return toByteBuffer((byte) (val + 1));
    }

}

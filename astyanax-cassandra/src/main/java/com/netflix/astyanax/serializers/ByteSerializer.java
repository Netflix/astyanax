package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.IntegerType;

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
        if (byteBuffer == null) {
            return null;
        }
        ByteBuffer dup = byteBuffer.duplicate();
        byte in = dup.get();
        return in;
    }

    @Override
    public ByteBuffer fromString(String str) {
        // Verify value is a short
        return toByteBuffer(Byte.parseByte(str));
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return IntegerType.instance.getString(byteBuffer);
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        Byte val = fromByteBuffer(byteBuffer.duplicate());
        if (val == Byte.MAX_VALUE) {
            throw new ArithmeticException("Can't paginate past max byte");
        }
        val++;
        return toByteBuffer(val);
    }

}

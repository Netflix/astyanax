package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.BytesType;

import com.netflix.astyanax.Serializer;

/**
 * A BytesArraySerializer translates the byte[] to and from ByteBuffer.
 * 
 * @author Patricio Echague
 * 
 */
public final class BytesArraySerializer extends AbstractSerializer<byte[]> implements Serializer<byte[]> {

    private static final BytesArraySerializer instance = new BytesArraySerializer();

    public static BytesArraySerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(byte[] obj) {
        if (obj == null) {
            return null;
        }
        return ByteBuffer.wrap(obj);
    }

    @Override
    public byte[] fromByteBuffer(ByteBuffer byteBuffer) {
            if (byteBuffer == null) {
                return null;
            }
            ByteBuffer dup = byteBuffer.duplicate();
            byte[] bytes = new byte[dup.remaining()];
            byteBuffer.get(bytes, 0, bytes.length);
            return bytes;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return BytesType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
	    if (byteBuffer == null) return null;
            return BytesType.instance.getString(byteBuffer.duplicate());
    }
}

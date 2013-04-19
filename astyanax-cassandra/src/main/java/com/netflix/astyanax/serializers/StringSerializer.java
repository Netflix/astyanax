package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.cassandra.db.marshal.UTF8Type;

/**
 * A StringSerializer translates the byte[] to and from string using utf-8
 * encoding.
 * 
 * @author Ran Tavory
 * 
 */
public final class StringSerializer extends AbstractSerializer<String> {

    private static final String UTF_8 = "UTF-8";
    private static final StringSerializer instance = new StringSerializer();
    private static final Charset charset = Charset.forName(UTF_8);

    public static StringSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(String obj) {
        if (obj == null) {
            return null;
        }
        return ByteBuffer.wrap(obj.getBytes(charset));
    }

    @Override
    public String fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        final ByteBuffer dup = byteBuffer.duplicate();
        return charset.decode(dup).toString();
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.UTF8TYPE;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return UTF8Type.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return UTF8Type.instance.getString(byteBuffer);
    }
}

package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

/**
 * Uses Char Serializer
 * 
 * @author Todd Nine
 */
public class CharSerializer extends AbstractSerializer<Character> {

    private static final CharSerializer instance = new CharSerializer();

    public static CharSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Character obj) {
        if (obj == null)
            return null;

        ByteBuffer buffer = ByteBuffer.allocate(Character.SIZE / Byte.SIZE);

        buffer.putChar(obj);
        buffer.rewind();

        return buffer;
    }

    @Override
    public Character fromByteBuffer(ByteBuffer bytes) {
        if (bytes == null) {
            return null;
        }
        ByteBuffer dup = bytes.duplicate();
        return dup.getChar();
    }

    @Override
    public ByteBuffer fromString(String str) {
        if (str == null || str.length() == 0)
            return null;
        return toByteBuffer(str.charAt(0));
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return fromByteBuffer(byteBuffer).toString();
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        throw new IllegalStateException("Char columns can't be paginated");
    }
}
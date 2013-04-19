package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.FloatType;

/**
 * Uses IntSerializer via translating Float objects to and from raw long bytes
 * form.
 * 
 * @author Todd Nine
 */
public class FloatSerializer extends AbstractSerializer<Float> {

    private static final FloatSerializer instance = new FloatSerializer();

    public static FloatSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Float obj) {
        return IntegerSerializer.get().toByteBuffer(
                Float.floatToRawIntBits(obj));
    }

    @Override
    public Float fromByteBuffer(ByteBuffer bytes) {
        if (bytes == null)
            return null;
        ByteBuffer dup = bytes.duplicate();
        return Float
                .intBitsToFloat(IntegerSerializer.get().fromByteBuffer(dup));
    }

    @Override
    public ByteBuffer fromString(String str) {
        return FloatType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return FloatType.instance.getString(byteBuffer);
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        float val = fromByteBuffer(byteBuffer.duplicate());
        if (val == Float.MAX_VALUE) {
            throw new ArithmeticException("Can't paginate past max float");
        }
        return toByteBuffer(val + Float.MIN_VALUE);
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.FLOATTYPE;
    }
}

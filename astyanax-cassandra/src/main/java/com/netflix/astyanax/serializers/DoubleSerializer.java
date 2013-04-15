package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.DoubleType;

/**
 * Uses LongSerializer via translating Doubles to and from raw long bytes form.
 * 
 * @author Yuri Finkelstein
 */
public class DoubleSerializer extends AbstractSerializer<Double> {

    private static final DoubleSerializer instance = new DoubleSerializer();

    public static DoubleSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Double obj) {
        return LongSerializer.get().toByteBuffer(
                Double.doubleToRawLongBits(obj));
    }

    @Override
    public Double fromByteBuffer(ByteBuffer bytes) {
        if (bytes == null)
            return null;
        ByteBuffer dup = bytes.duplicate();
        return Double
                .longBitsToDouble(LongSerializer.get().fromByteBuffer(dup));
    }

    @Override
    public ByteBuffer fromString(String str) {
        return DoubleType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return DoubleType.instance.getString(byteBuffer);
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        double val = fromByteBuffer(byteBuffer.duplicate());
        if (val == Double.MAX_VALUE) {
            throw new ArithmeticException("Can't paginate past max double");
        }
        return toByteBuffer(val + Double.MIN_VALUE);
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.DOUBLETYPE;
    }
}

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
        return LongSerializer.get().toByteBuffer(Double.doubleToRawLongBits(obj));
    }

    @Override
    public Double fromByteBuffer(ByteBuffer bytes) {
        try {
            return Double.longBitsToDouble(LongSerializer.get().fromByteBuffer(
                    bytes));
        } finally {
            if (bytes != null)
                bytes.rewind();
        }
    }

    @Override
    public ByteBuffer fromString(String str) {
        return DoubleType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        try {
            return DoubleType.instance.getString(byteBuffer);
        } finally {
            if (byteBuffer != null)
                byteBuffer.rewind();
        }
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        try {
            double val = fromByteBuffer(byteBuffer.duplicate());
            if (val == Double.MAX_VALUE) {
                throw new ArithmeticException("Can't paginate past max double");
            }
            return toByteBuffer(val + Double.MIN_VALUE);
        } finally {
            if (byteBuffer != null)
                byteBuffer.rewind();
        }
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.DOUBLETYPE;
    }
}

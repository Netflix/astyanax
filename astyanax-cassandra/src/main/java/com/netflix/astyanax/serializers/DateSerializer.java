package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.cassandra.db.marshal.DateType;

/**
 * Converts bytes to Date and vice versa, by first converting the Date to or
 * from a long which represents the specified number of milliseconds since the
 * standard base time known as "the Unix epoch", that is January 1, 1970,
 * 00:00:00 UTC.
 * 
 * @author Jim Ancona
 * @see java.util.Date
 */
public final class DateSerializer extends AbstractSerializer<Date> {
    private static final LongSerializer LONG_SERIALIZER = LongSerializer.get();
    private static final DateSerializer instance = new DateSerializer();

    public static DateSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Date obj) {
        if (obj == null) {
            return null;
        }
        return LONG_SERIALIZER.toByteBuffer(obj.getTime());
    }

    @Override
    public Date fromByteBuffer(ByteBuffer bytes) {
        try {
            if (bytes == null) {
                return null;
            }
            return new Date(LONG_SERIALIZER.fromByteBuffer(bytes));
        } finally {
            if (bytes != null)
                bytes.rewind();
        }
    }

    @Override
    public ByteBuffer fromString(String str) {
        return DateType.instance.fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        try {
            return DateType.instance.getString(byteBuffer);
        } finally {
            if (byteBuffer != null)
                byteBuffer.rewind();
        }
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        try {
            return toByteBuffer(new Date(
                    fromByteBuffer(byteBuffer).getTime() + 1));
        } finally {
            if (byteBuffer != null)
                byteBuffer.rewind();
        }
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.DATETYPE;
    }

}

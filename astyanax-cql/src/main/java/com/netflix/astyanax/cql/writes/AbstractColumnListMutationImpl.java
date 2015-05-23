package com.netflix.astyanax.cql.writes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.StringUtils;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.ByteSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.FloatSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.ShortSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

public abstract class AbstractColumnListMutationImpl<C> implements ColumnListMutation<C> {
    
	protected final AtomicReference<Long> defaultTimestamp = new AtomicReference<Long>(null);
	protected final AtomicReference<Integer> defaultTTL = new AtomicReference<Integer>(null);

    public AbstractColumnListMutationImpl(Long newTimestamp) {
        this.defaultTimestamp.set(newTimestamp);
    }
    
    @Override
    public ColumnListMutation<C> putColumn(C columnName, String value, Integer ttl) {
        return putColumn(columnName, value, StringSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final String value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, String value) {
        if (value != null) {
            return putColumn(columnName, value);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, String value, Integer ttl) {
        if (value != null) {
            return putColumn(columnName, value, ttl);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, byte[] value, Integer ttl) {
        return putColumn(columnName, value, BytesArraySerializer.get(), ttl);
    }

    @Override
    public <V> ColumnListMutation<C> putColumnIfNotNull(C columnName, V value, Serializer<V> valueSerializer, Integer ttl) {
        if (value == null)
            return this;
        return putColumn(columnName, value, valueSerializer, ttl);
    }
    
    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final byte[] value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, byte[] value) {
        if (value != null) {
            return putColumn(columnName, value);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, byte[] value, Integer ttl) {
        if (value != null) {
            return putColumn(columnName, value, ttl);
        }
        return this;
    }
    
    @Override
    public ColumnListMutation<C> putColumn(C columnName, byte value, Integer ttl) {
        return putColumn(columnName, value, ByteSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final byte value) {
        return putColumn(columnName, value, null);
    }
    
    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Byte value, Integer ttl) {
        if (value != null) {
            return putColumn(columnName, value, ttl);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Byte value) {
        if (value != null) {
            return putColumn(columnName, value);
        }
        return this;
    }
    
    @Override
    public ColumnListMutation<C> putColumn(C columnName, short value, Integer ttl) {
        return putColumn(columnName, value, ShortSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final short value) {
        return putColumn(columnName, value, null);
    }
    
    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Short value, Integer ttl) {
        if (value != null) {
            return putColumn(columnName, value, ttl);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Short value) {
        if (value != null) {
            return putColumn(columnName, value);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, int value, Integer ttl) {
        return putColumn(columnName, value, IntegerSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final int value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Integer value) {
        if (value != null) {
            return putColumn(columnName, value);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Integer value, Integer ttl) {
        if (value != null) {
            return putColumn(columnName, value, ttl);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, long value, Integer ttl) {
        return putColumn(columnName, value, LongSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final long value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Long value) {
        if (value != null) {
            return putColumn(columnName, value);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Long value, Integer ttl) {
        if (value != null) {
            return putColumn(columnName, value, ttl);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, boolean value, Integer ttl) {
        return putColumn(columnName, value, BooleanSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final boolean value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Boolean value) {
        if (value != null) {
            return putColumn(columnName, value);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Boolean value, Integer ttl) {
        if (value != null) {
            return putColumn(columnName, value, ttl);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, ByteBuffer value, Integer ttl) {
        return putColumn(columnName, value, ByteBufferSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final ByteBuffer value) {
        return putColumn(columnName, value, null);
    }
    
    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, ByteBuffer value) {
        if (value != null) {
            return putColumn(columnName, value);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, ByteBuffer value, Integer ttl) {
        if (value != null) {
            return putColumn(columnName, value, ttl);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, Date value, Integer ttl) {
        return putColumn(columnName, value, DateSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final Date value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Date value) {
        if (value != null) {
            return putColumn(columnName, value);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Date value, Integer ttl) {
        if (value != null) {
            return putColumn(columnName, value, ttl);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, float value, Integer ttl) {
        return putColumn(columnName, value, FloatSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final float value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Float value) {
        if (value != null) {
            return putColumn(columnName, value);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Float value, Integer ttl) {
        if (value != null) {
            return putColumn(columnName, value, ttl);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, double value, Integer ttl) {
        return putColumn(columnName, value, DoubleSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final double value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Double value) {
        if (value != null) {
            return putColumn(columnName, value);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Double value, Integer ttl) {
        if (value != null) {
            return putColumn(columnName, value, ttl);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, UUID value, Integer ttl) {
        return putColumn(columnName, value, UUIDSerializer.get(), ttl);
    }

    @Override
    public ColumnListMutation<C> putColumn(final C columnName, final UUID value) {
        return putColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, UUID value) {
        if (value != null) {
            return putColumn(columnName, value);
        }
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, UUID value, Integer ttl) {
        if (value != null) {
            return putColumn(columnName, value, ttl);
        }
        return this;
    }
    
    @Override
    public ColumnListMutation<C> putEmptyColumn(final C columnName) {
        return putEmptyColumn(columnName, null);
    }

    @Override
    public ColumnListMutation<C> setTimestamp(long timestamp) {
        this.defaultTimestamp.set(timestamp);
        return this;
    }

    @Override
    public ColumnListMutation<C> putCompressedColumn(C columnName, String value, Integer ttl) {
        Preconditions.checkNotNull(value, "Can't insert null value");
        
        if (value == null) {
            putEmptyColumn(columnName, ttl);
            return this;
        }
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(StringUtils.getBytesUtf8(value));
            gzip.close();
            return this.putColumn(columnName, ByteBuffer.wrap(out.toByteArray()), ttl);
        } catch (IOException e) {
            throw new RuntimeException("Error compressing column " + columnName, e);
        }
    }

    @Override
    public ColumnListMutation<C> putCompressedColumn(C columnName, String value) {
        return putCompressedColumn(columnName, value, null);
    }

    @Override
    public ColumnListMutation<C> putCompressedColumnIfNotNull(C columnName, String value, Integer ttl) {
        if (value == null)
            return this;
        return putCompressedColumn(columnName, value, ttl);
    }

    @Override
    public ColumnListMutation<C> putCompressedColumnIfNotNull(C columnName, String value) {
        if (value == null)
            return this;
        return putCompressedColumn(columnName, value);
    }

    public Integer getDefaultTtl() {
        return defaultTTL.get();
    }
    
    public Long getTimestamp() {
        return defaultTimestamp.get();
    }
}

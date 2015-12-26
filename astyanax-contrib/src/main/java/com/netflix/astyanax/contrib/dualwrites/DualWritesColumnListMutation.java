package com.netflix.astyanax.contrib.dualwrites;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ColumnPath;

/**
 * Class that implements the {@link ColumnListMutation} interface and acts as a dual router for capturing all the dual writes. 
 * Note that it purely maintains state in 2 separate ColumnListMutation objects, each corresponding to the source of destination keyspace / mutation batches
 * 
 * @author poberai
 *
 * @param <C>
 */
@SuppressWarnings("deprecation")
public class DualWritesColumnListMutation<C> implements ColumnListMutation<C> {

    private final ColumnListMutation<C> primary;
    private final ColumnListMutation<C> secondary;

    public DualWritesColumnListMutation(ColumnListMutation<C> primaryClm, ColumnListMutation<C> secondaryClm) {
        primary = primaryClm;
        secondary = secondaryClm;
    }

    @Override
    public <V> ColumnListMutation<C> putColumn(C columnName, V value, Serializer<V> valueSerializer, Integer ttl) {
        primary.putColumn(columnName, value, valueSerializer, ttl);
        secondary.putColumn(columnName, value, valueSerializer, ttl);
        return this;
    }

    @Override
    public <V> ColumnListMutation<C> putColumnIfNotNull(C columnName, V value, Serializer<V> valueSerializer, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, valueSerializer, ttl);
        secondary.putColumnIfNotNull(columnName, value, valueSerializer, ttl);
        return this;
    }

    @Override
    public <SC> ColumnListMutation<SC> withSuperColumn(ColumnPath<SC> superColumnPath) {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, String value, Integer ttl) {
        primary.putColumn(columnName, value, ttl);
        secondary.putColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, String value) {
        primary.putColumn(columnName, value);
        secondary.putColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, String value, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, ttl);
        secondary.putColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, String value) {
        primary.putColumnIfNotNull(columnName, value);
        secondary.putColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putCompressedColumn(C columnName, String value, Integer ttl) {
        primary.putCompressedColumn(columnName, value, ttl);
        secondary.putCompressedColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putCompressedColumn(C columnName, String value) {
        primary.putCompressedColumn(columnName, value);
        secondary.putCompressedColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putCompressedColumnIfNotNull(C columnName, String value, Integer ttl) {
        primary.putCompressedColumnIfNotNull(columnName, value, ttl);
        secondary.putCompressedColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putCompressedColumnIfNotNull(C columnName, String value) {
        primary.putCompressedColumnIfNotNull(columnName, value);
        secondary.putCompressedColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, byte[] value, Integer ttl) {
        primary.putColumn(columnName, value, ttl);
        secondary.putColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, byte[] value) {
        primary.putColumn(columnName, value);
        secondary.putColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, byte[] value, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, ttl);
        secondary.putColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, byte[] value) {
        primary.putColumnIfNotNull(columnName, value);
        secondary.putColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, byte value, Integer ttl) {
        primary.putColumn(columnName, value, ttl);
        secondary.putColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, byte value) {
        primary.putColumn(columnName, value);
        secondary.putColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Byte value, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, ttl);
        secondary.putColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Byte value) {
        primary.putColumnIfNotNull(columnName, value);
        secondary.putColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, short value, Integer ttl) {
        primary.putColumn(columnName, value, ttl);
        secondary.putColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, short value) {
        primary.putColumn(columnName, value);
        secondary.putColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Short value, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, ttl);
        secondary.putColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Short value) {
        primary.putColumnIfNotNull(columnName, value);
        secondary.putColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, int value, Integer ttl) {
        primary.putColumn(columnName, value, ttl);
        secondary.putColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, int value) {
        primary.putColumn(columnName, value);
        secondary.putColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Integer value, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, ttl);
        secondary.putColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Integer value) {
        primary.putColumnIfNotNull(columnName, value);
        secondary.putColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, long value, Integer ttl) {
        primary.putColumn(columnName, value, ttl);
        secondary.putColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, long value) {
        primary.putColumn(columnName, value);
        secondary.putColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Long value, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, ttl);
        secondary.putColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Long value) {
        primary.putColumnIfNotNull(columnName, value);
        secondary.putColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, boolean value, Integer ttl) {
        primary.putColumn(columnName, value, ttl);
        secondary.putColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, boolean value) {
        primary.putColumn(columnName, value);
        secondary.putColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Boolean value, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, ttl);
        secondary.putColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Boolean value) {
        primary.putColumnIfNotNull(columnName, value);
        secondary.putColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, ByteBuffer value, Integer ttl) {
        primary.putColumn(columnName, value, ttl);
        secondary.putColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, ByteBuffer value) {
        primary.putColumn(columnName, value);
        secondary.putColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, ByteBuffer value, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, ttl);
        secondary.putColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, ByteBuffer value) {
        primary.putColumnIfNotNull(columnName, value);
        secondary.putColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, Date value, Integer ttl) {
        primary.putColumn(columnName, value, ttl);
        secondary.putColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, Date value) {
        primary.putColumn(columnName, value);
        secondary.putColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Date value, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, ttl);
        secondary.putColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Date value) {
        primary.putColumnIfNotNull(columnName, value);
        secondary.putColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, float value, Integer ttl) {
        primary.putColumn(columnName, value, ttl);
        secondary.putColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, float value) {
        primary.putColumn(columnName, value);
        secondary.putColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Float value, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, ttl);
        secondary.putColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Float value) {
        primary.putColumnIfNotNull(columnName, value);
        secondary.putColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, double value, Integer ttl) {
        primary.putColumn(columnName, value, ttl);
        secondary.putColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, double value) {
        primary.putColumn(columnName, value);
        secondary.putColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Double value, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, ttl);
        secondary.putColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, Double value) {
        primary.putColumnIfNotNull(columnName, value);
        secondary.putColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, UUID value, Integer ttl) {
        primary.putColumn(columnName, value, ttl);
        secondary.putColumn(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumn(C columnName, UUID value) {
        primary.putColumn(columnName, value);
        secondary.putColumn(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, UUID value, Integer ttl) {
        primary.putColumnIfNotNull(columnName, value, ttl);
        secondary.putColumnIfNotNull(columnName, value, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putColumnIfNotNull(C columnName, UUID value) {
        primary.putColumnIfNotNull(columnName, value);
        secondary.putColumnIfNotNull(columnName, value);
        return this;
    }

    @Override
    public ColumnListMutation<C> putEmptyColumn(C columnName, Integer ttl) {
        primary.putEmptyColumn(columnName, ttl);
        secondary.putEmptyColumn(columnName, ttl);
        return this;
    }

    @Override
    public ColumnListMutation<C> putEmptyColumn(C columnName) {
        primary.putEmptyColumn(columnName);
        secondary.putEmptyColumn(columnName);
        return this;
    }

    @Override
    public ColumnListMutation<C> incrementCounterColumn(C columnName, long amount) {
        primary.incrementCounterColumn(columnName, amount);
        secondary.incrementCounterColumn(columnName, amount);
        return this;
    }

    @Override
    public ColumnListMutation<C> deleteColumn(C columnName) {
        primary.deleteColumn(columnName);
        secondary.deleteColumn(columnName);
        return this;
    }

    @Override
    public ColumnListMutation<C> setTimestamp(long timestamp) {
        primary.setTimestamp(timestamp);
        secondary.setTimestamp(timestamp);
        return this;
    }

    @Override
    public ColumnListMutation<C> delete() {
        primary.delete();
        secondary.delete();
        return this;
    }

    @Override
    public ColumnListMutation<C> setDefaultTtl(Integer ttl) {
        primary.setDefaultTtl(ttl);
        secondary.setDefaultTtl(ttl);
        return this;
    }

}

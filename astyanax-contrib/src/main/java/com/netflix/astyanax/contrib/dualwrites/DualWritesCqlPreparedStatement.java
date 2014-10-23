package com.netflix.astyanax.contrib.dualwrites;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlPreparedStatement;
import com.netflix.astyanax.cql.CqlStatementResult;

public class DualWritesCqlPreparedStatement implements CqlPreparedStatement {

    private final CqlPreparedStatement primary;
    private final CqlPreparedStatement secondary;
    private final DualWritesStrategy execStrategy;
    private final DualKeyspaceMetadata ksMd;

    public DualWritesCqlPreparedStatement(CqlPreparedStatement primaryCql, CqlPreparedStatement secondarycql, DualWritesStrategy strategy, DualKeyspaceMetadata keyspaceMd) {
        primary = primaryCql;
        secondary = secondarycql;
        execStrategy = strategy;
        ksMd = keyspaceMd;
    }

    @Override
    public <V> CqlPreparedStatement withByteBufferValue(V value, Serializer<V> serializer) {
        primary.withByteBufferValue(value, serializer);
        secondary.withByteBufferValue(value, serializer);
        return this;
    }

    @Override
    public CqlPreparedStatement withValue(ByteBuffer value) {
        primary.withValue(value);
        secondary.withValue(value);
        return this;
    }

    @Override
    public CqlPreparedStatement withValues(List<ByteBuffer> value) {
        primary.withValues(value);
        secondary.withValues(value);
        return this;
    }

    @Override
    public CqlPreparedStatement withStringValue(String value) {
        primary.withStringValue(value);
        secondary.withStringValue(value);
        return this;
    }

    @Override
    public CqlPreparedStatement withIntegerValue(Integer value) {
        primary.withIntegerValue(value);
        secondary.withIntegerValue(value);
        return this;
    }

    @Override
    public CqlPreparedStatement withBooleanValue(Boolean value) {
        primary.withBooleanValue(value);
        secondary.withBooleanValue(value);
        return this;
    }

    @Override
    public CqlPreparedStatement withDoubleValue(Double value) {
        primary.withDoubleValue(value);
        secondary.withDoubleValue(value);
        return this;
    }

    @Override
    public CqlPreparedStatement withLongValue(Long value) {
        primary.withLongValue(value);
        secondary.withLongValue(value);
        return this;
    }

    @Override
    public CqlPreparedStatement withFloatValue(Float value) {
        primary.withFloatValue(value);
        secondary.withFloatValue(value);
        return this;
    }

    @Override
    public CqlPreparedStatement withShortValue(Short value) {
        primary.withShortValue(value);
        secondary.withShortValue(value);
        return this;
    }

    @Override
    public CqlPreparedStatement withUUIDValue(UUID value) {
        primary.withUUIDValue(value);
        secondary.withUUIDValue(value);
        return this;
    }

    @Override
    public OperationResult<CqlStatementResult> execute() throws ConnectionException {
        WriteMetadata writeMd = new WriteMetadata(ksMd, null, null);
        return execStrategy.wrapExecutions(primary, secondary, Collections.singletonList(writeMd)).execute();
    }

    @Override
    public ListenableFuture<OperationResult<CqlStatementResult>> executeAsync() throws ConnectionException {
        WriteMetadata writeMd = new WriteMetadata(ksMd, null, null);
        return execStrategy.wrapExecutions(primary, secondary, Collections.singletonList(writeMd)).executeAsync();
    }
}

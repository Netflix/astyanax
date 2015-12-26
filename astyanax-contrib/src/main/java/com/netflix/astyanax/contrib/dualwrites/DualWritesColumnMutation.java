package com.netflix.astyanax.contrib.dualwrites;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Class that implements the {@link ColumnMutation} interface and acts as a dual router for capturing all the dual writes. 
 * Note that it purely maintains state in 2 separate ColumnMutation objects, each corresponding to the source of destination keyspace
 * 
 * @author poberai
 *
 * @param <C>
 */
public class DualWritesColumnMutation implements ColumnMutation {

    private final ColumnMutation primary;
    private final ColumnMutation secondary;
    private final DualWritesStrategy executionStrategy;
    private final Collection<WriteMetadata> writeMetadata;
    
    public DualWritesColumnMutation(WriteMetadata writeMD, ColumnMutation primaryClm, ColumnMutation secondaryClm, DualWritesStrategy execStrategy) {
        writeMetadata = Collections.singletonList(writeMD);
        primary = primaryClm;
        secondary = secondaryClm;
        executionStrategy = execStrategy;
    }

    @Override
    public ColumnMutation setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        primary.setConsistencyLevel(consistencyLevel);
        secondary.setConsistencyLevel(consistencyLevel);
        return this;
    }

    @Override
    public ColumnMutation withRetryPolicy(RetryPolicy retry) {
        primary.withRetryPolicy(retry);
        secondary.withRetryPolicy(retry);
        return this;
    }

    @Override
    public ColumnMutation withTimestamp(long timestamp) {
        primary.withTimestamp(timestamp);
        secondary.withTimestamp(timestamp);
        return this;
    }

    @Override
    public Execution<Void> putValue(String value, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, ttl);
        Execution<Void> ex2 = secondary.putValue(value, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> putValue(byte[] value, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, ttl);
        Execution<Void> ex2 = secondary.putValue(value, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> putValue(byte value, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, ttl);
        Execution<Void> ex2 = secondary.putValue(value, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> putValue(short value, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, ttl);
        Execution<Void> ex2 = secondary.putValue(value, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> putValue(int value, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, ttl);
        Execution<Void> ex2 = secondary.putValue(value, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> putValue(long value, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, ttl);
        Execution<Void> ex2 = secondary.putValue(value, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> putValue(boolean value, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, ttl);
        Execution<Void> ex2 = secondary.putValue(value, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> putValue(ByteBuffer value, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, ttl);
        Execution<Void> ex2 = secondary.putValue(value, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> putValue(Date value, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, ttl);
        Execution<Void> ex2 = secondary.putValue(value, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> putValue(float value, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, ttl);
        Execution<Void> ex2 = secondary.putValue(value, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> putValue(double value, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, ttl);
        Execution<Void> ex2 = secondary.putValue(value, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> putValue(UUID value, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, ttl);
        Execution<Void> ex2 = secondary.putValue(value, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public <T> Execution<Void> putValue(T value, Serializer<T> serializer, Integer ttl) {
        Execution<Void> ex1 = primary.putValue(value, serializer, ttl);
        Execution<Void> ex2 = secondary.putValue(value, serializer, ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> putEmptyColumn(Integer ttl) {
        Execution<Void> ex1 = primary.putEmptyColumn(ttl);
        Execution<Void> ex2 = secondary.putEmptyColumn(ttl);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> incrementCounterColumn(long amount) {
        Execution<Void> ex1 = primary.incrementCounterColumn(amount);
        Execution<Void> ex2 = secondary.incrementCounterColumn(amount);
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> deleteColumn() {
        Execution<Void> ex1 = primary.deleteColumn();
        Execution<Void> ex2 = secondary.deleteColumn();
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }

    @Override
    public Execution<Void> deleteCounterColumn() {
        Execution<Void> ex1 = primary.deleteCounterColumn();
        Execution<Void> ex2 = secondary.deleteCounterColumn();
        return executionStrategy.wrapExecutions(ex1, ex2, writeMetadata);
    }
}

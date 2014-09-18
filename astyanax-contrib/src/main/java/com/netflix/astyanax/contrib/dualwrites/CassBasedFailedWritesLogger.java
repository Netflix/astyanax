package com.netflix.astyanax.contrib.dualwrites;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class CassBasedFailedWritesLogger implements FailedWritesLogger {

    private static final Logger Logger = LoggerFactory.getLogger(CassBasedFailedWritesLogger.class);
    
    private final AstyanaxContext<Keyspace> ksContext; 
    private Keyspace ks;
    private final CircularCounter counter;
    
    public CassBasedFailedWritesLogger(AstyanaxContext<Keyspace> ctx) {
        this(ctx, 10);
    }
    
    public CassBasedFailedWritesLogger(AstyanaxContext<Keyspace> ctx, int numShards) {
        this.ksContext = ctx;
        this.counter = new CircularCounter(numShards);
    }

    @Override
    public void logFailedWrite(WriteMetadata failedWrite) {
        
        MutationBatch mutationBatch = ks.prepareMutationBatch();
        addToBatch(mutationBatch, failedWrite);
        
        try {
            mutationBatch.execute();
        } catch (ConnectionException e) {
            Logger.error("Failed to log failed write to fallback cluster: " + failedWrite, e);
        }
    }
    
    private void addToBatch(MutationBatch batch, WriteMetadata failedWrite) {
        
        String cfName = failedWrite.getPrimaryCluster() + "-" + failedWrite.getPrimaryKeyspace();
        
        ColumnFamily<String, Long> CF_FAILED_WRITES = 
                ColumnFamily.newColumnFamily(cfName, StringSerializer.get(), LongSerializer.get(), StringSerializer.get());
        
        String rowKey = failedWrite.getCFName() + "_" + counter.getNext();
        Long column = failedWrite.getUuid();
        String value = failedWrite.getRowKey();
        
        batch.withRow(CF_FAILED_WRITES, rowKey).putColumn(column, value);
    }

    private class CircularCounter { 
        
        private final int maxLimit;
        private final AtomicInteger counter = new AtomicInteger(0);
        
        private CircularCounter(int limit) {
            maxLimit = limit;
        }
        
        private int getNext() {
            int count = counter.incrementAndGet();
            return (count % maxLimit);
        }
    }


    @Override
    public void init() {
        ks = ksContext.getClient();
        ksContext.start();
    }

    @Override
    public void shutdown() {
        ksContext.shutdown();
    }
}

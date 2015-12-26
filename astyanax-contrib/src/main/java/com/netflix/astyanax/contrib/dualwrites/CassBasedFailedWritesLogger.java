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

/**
 * Impl of {@link FailedWritesLogger} which communicates metadata of failed writes to a separate cluster. 
 * The keyspace and cluster for this backup cassandra cluster needs to be provided to this class. 
 * Note that for ease of management, the original cluster and keysapce are represented in the backup CF name. 
 * Row keys are sharded between 0 ... 10 so that there is no hot spot and also represent the CF name in there for ease of management. 
 * 
 * NOTE: this class only backs up metadata about the failed write - i.e not the actual payload. 
 * This serves merely as an indicator of which rows in which cluster / keysapce / CF were not sent to the destination cluster / keyspace / CF. 
 * 
 * @author poberai
 *
 */
public class CassBasedFailedWritesLogger implements FailedWritesLogger {

    private static final Logger Logger = LoggerFactory.getLogger(CassBasedFailedWritesLogger.class);
    
    private final AstyanaxContext<Keyspace> ksContext; 
    private Keyspace ks;
    private final CircularCounter counter;
    
    public CassBasedFailedWritesLogger(AstyanaxContext<Keyspace> ctx) {
        this(ctx, 10);
    }
    
    public CassBasedFailedWritesLogger(AstyanaxContext<Keyspace> ctx, int numShards) {
        if (numShards <= 0) {
            throw new RuntimeException("numShards must be > 0");
        }
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
        
        // TODO: must deal with failed operations like createCF etc
        if (failedWrite.getCFName() == null || failedWrite.getRowKey() == null) {
            return;
        }
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

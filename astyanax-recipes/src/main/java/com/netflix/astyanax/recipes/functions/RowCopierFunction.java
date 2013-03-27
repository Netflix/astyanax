package com.netflix.astyanax.recipes.functions;

import java.io.Flushable;
import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Sets;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;

/**
 * Function to copy rows into a target column family
 * 
 * TODO:  Failover, retry
 *  
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public class RowCopierFunction<K,C> implements Function<Row<K,C>, Boolean>, Flushable {
    private static final Logger LOG = LoggerFactory.getLogger(RowCopierFunction.class);
    
    private static final int DEFAULT_BATCH_SIZE = 100;
    
    public static class Builder<K,C> {
        private final ColumnFamily<K,C> columnFamily;
        private final Keyspace          keyspace;
        private       int               batchSize           = DEFAULT_BATCH_SIZE;
        
        public Builder(Keyspace keyspace, ColumnFamily<K,C> columnFamily) {
            this.columnFamily = columnFamily;
            this.keyspace     = keyspace;
        }
        
        public Builder<K,C> withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }
        
        public RowCopierFunction<K,C> build() {
            return new RowCopierFunction<K,C>(this);
        }
    }

    public static <K, C> Builder<K,C> builder(Keyspace keyspace, ColumnFamily<K,C> columnFamily) {
        return new Builder<K,C>(keyspace, columnFamily);
    }
    
    private final ColumnFamily<K,C> columnFamily;
    private final Keyspace          keyspace;
    private final int               batchSize;
    private final ThreadLocal<ThreadContext> context = new ThreadLocal<ThreadContext>();
    private final Set<ThreadContext> contexts = Sets.newIdentityHashSet();
    
    private static class ThreadContext {
        MutationBatch mb;
        int counter = 0;
    }
    
    private RowCopierFunction(Builder<K,C> builder) {
        this.columnFamily = builder.columnFamily;
        this.batchSize    = builder.batchSize;
        this.keyspace     = builder.keyspace;
    }
    
    @Override
    public Boolean apply(Row<K, C> row) {
        ThreadContext context = this.context.get();
        if (context == null) {
            context = new ThreadContext();
            context.mb = keyspace.prepareMutationBatch();
            this.context.set(context);
            
            synchronized (this) {
                contexts.add(context);
            }
        }
        
        ColumnListMutation<C> mbRow = context.mb.withRow(columnFamily, row.getKey());
        context.mb.lockCurrentTimestamp();
        for (Column<C> column : row.getColumns()) {
            mbRow.setTimestamp(column.getTimestamp());
            mbRow.putColumn(column.getName(), column.getByteBufferValue(), column.getTtl());
        }
        
        context.counter++;
        if (context.counter == batchSize) {
            try {
                context.mb.execute();
                context.counter = 0;
            }
            catch (Exception e) {
                LOG.error("Failed to write mutation", e);
                return false;
            }
            
        }
        return true;
    }

    @Override
    public void flush() throws IOException {
        for (ThreadContext context : contexts) {
            try {
                context.mb.execute();
            } catch (ConnectionException e) {
                LOG.error("Failed to write mutation", e);
            }
        }
    }

}

package com.netflix.astyanax.recipes;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.CompositeParser;
import com.netflix.astyanax.model.Composites;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.util.RangeBuilder;

/**
 * Performs a search on a reverse index and fetches all the matching rows
 * 
 * CFData:K C=V1 C=V2
 * 
 * CFIndex: V1:K
 * 
 * <h3>Data and Index column family</h3> The CFData column family has key of
 * type K and fields or columns of type C. Each column may have a different
 * value type. The CFIndex column family is a sorted index by one of the value
 * types V. The column names in the reverse index are a composite of the value
 * type V and the CFData rowkey type K (V:K).
 * 
 * @author elandau
 * 
 * @param <K>
 *            Key type for data table
 * @param <C>
 *            Column name type for data table
 * @param <V>
 *            Value type being indexed
 */
public class ReverseIndexQuery<K, C, V> {

    public static <K, C, V> ReverseIndexQuery<K, C, V> newQuery(Keyspace ks, ColumnFamily<K, C> cf, String indexCf,
            Serializer<V> valSerializer) {
        return new ReverseIndexQuery<K, C, V>(ks, cf, indexCf, valSerializer);
    }

    public static interface IndexEntryCallback<K, V> {
        boolean handleEntry(K key, V value, ByteBuffer meta);
    }

    private final Keyspace ks;
    private final ColumnFamily<K, C> cfData;
    private final Serializer<V> valSerializer;
    private Collection<ByteBuffer> shardKeys;
    private final ColumnFamily<ByteBuffer, ByteBuffer> cfIndex;
    private ExecutorService executor;
    private V startValue;
    private V endValue;
    private int keyLimit = 100;
    private int columnLimit = 1000;
    private int shardColumnLimit = 0;
    private final AtomicLong pendingTasks = new AtomicLong();
    private Function<Row<K, C>, Void> callback;
    private IndexEntryCallback<K, V> indexCallback;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.CL_ONE;
    private RetryPolicy retry = RunOnce.get();
    private Collection<C> columnSlice;
    private CountDownLatch latch = new CountDownLatch(1);

    public ReverseIndexQuery(Keyspace ks, ColumnFamily<K, C> cfData, String indexCf, Serializer<V> valSerializer) {
        this.ks = ks;
        this.cfData = cfData;
        this.valSerializer = valSerializer;
        this.startValue = null;
        this.endValue = null;
        this.cfIndex = ColumnFamily.newColumnFamily(indexCf, ByteBufferSerializer.get(), ByteBufferSerializer.get());
    }

    public ReverseIndexQuery<K, C, V> useExecutor(ExecutorService executor) {
        this.executor = executor;
        return this;
    }

    public ReverseIndexQuery<K, C, V> useRetryPolicy(RetryPolicy retry) {
        this.retry = retry;
        return this;
    }

    public ReverseIndexQuery<K, C, V> withIndexShards(Collection<ByteBuffer> shardKeys) {
        this.shardKeys = shardKeys;
        return this;
    }

    public ReverseIndexQuery<K, C, V> fromIndexValue(V startValue) {
        this.startValue = startValue;
        return this;
    }

    public ReverseIndexQuery<K, C, V> toIndexValue(V endValue) {
        this.endValue = endValue;
        return this;
    }

    public ReverseIndexQuery<K, C, V> forEach(Function<Row<K, C>, Void> callback) {
        this.callback = callback;
        return this;
    }

    public ReverseIndexQuery<K, C, V> forEachIndexEntry(IndexEntryCallback<K, V> callback) {
        this.indexCallback = callback;
        return this;
    }

    public ReverseIndexQuery<K, C, V> withConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    public ReverseIndexQuery<K, C, V> withColumnSlice(Collection<C> columnSlice) {
        this.columnSlice = columnSlice;
        return this;
    }

    /**
     * Set the number shard keys to fetch for the first query
     * 
     * @param size
     * @return
     */
    public ReverseIndexQuery<K, C, V> setShardBlockSize(int size) {
        this.keyLimit = size;
        return this;
    }

    /**
     * Set the number columns to read from each shard when paginating.
     * 
     * @param size
     * @return
     */
    public ReverseIndexQuery<K, C, V> setShardPageSize(int size) {
        this.columnLimit = size;
        return this;
    }

    public ReverseIndexQuery<K, C, V> setShardNextPageSize(int size) {
        this.shardColumnLimit = size;
        return this;
    }

    public abstract class Task implements Runnable {
        public Task() {
            pendingTasks.incrementAndGet();
            executor.submit(this);
        }

        @Override
        public final void run() {
            try {
                internalRun();
            }
            catch (Throwable t) {
            }

            if (pendingTasks.decrementAndGet() == 0)
                latch.countDown();
        }

        protected abstract void internalRun();
    }

    public void execute() {
        if (executor == null)
            executor = Executors.newFixedThreadPool(5, new ThreadFactoryBuilder().setDaemon(true).build());

        // Break up the shards into batches
        List<ByteBuffer> batch = Lists.newArrayListWithCapacity(keyLimit);
        for (ByteBuffer shard : shardKeys) {
            batch.add(shard);
            if (batch.size() == keyLimit) {
                fetchFirstIndexBatch(batch);
                batch = Lists.newArrayListWithCapacity(keyLimit);
            }
        }
        if (!batch.isEmpty()) {
            fetchFirstIndexBatch(batch);
        }

        if (pendingTasks.get() > 0) {
            try {
                latch.await(1000, TimeUnit.MINUTES);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void fetchFirstIndexBatch(final Collection<ByteBuffer> keys) {
        new Task() {
            @Override
            protected void internalRun() {
                // Get the first range in the index
                RangeBuilder range = new RangeBuilder();
                if (startValue != null) {
                    range.setStart(Composites.newCompositeBuilder().greaterThanEquals().add(startValue, valSerializer)
                            .build());
                }
                if (endValue != null) {
                    range.setEnd(Composites.newCompositeBuilder().lessThanEquals().add(endValue, valSerializer).build());
                }

                // Read the index shards
                OperationResult<Rows<ByteBuffer, ByteBuffer>> result = null;
                try {
                    result = ks.prepareQuery(cfIndex).setConsistencyLevel(consistencyLevel).withRetryPolicy(retry)
                            .getKeySlice(keys).withColumnRange(range.setLimit(columnLimit).build()).execute();
                }
                catch (ConnectionException e) {
                    e.printStackTrace();
                    return;
                }

                // Read the actual data rows in batches
                List<K> batch = Lists.newArrayListWithCapacity(keyLimit);
                for (Row<ByteBuffer, ByteBuffer> row : result.getResult()) {
                    if (!row.getColumns().isEmpty()) {
                        V lastValue = null;
                        for (Column<ByteBuffer> column : row.getColumns()) {
                            CompositeParser parser = Composites.newCompositeParser(column.getName());
                            lastValue = parser.read(valSerializer);
                            K key = parser.read(cfData.getKeySerializer());

                            if (indexCallback != null) {
                                if (!indexCallback.handleEntry(key, lastValue, column.getByteBufferValue())) {
                                    continue;
                                }
                            }

                            if (callback != null) {
                                batch.add(key);

                                if (batch.size() == keyLimit) {
                                    fetchDataBatch(batch);
                                    batch = Lists.newArrayListWithCapacity(keyLimit);
                                }
                            }
                        }

                        if (row.getColumns().size() == columnLimit) {
                            paginateIndexShard(row.getKey(), lastValue);
                        }
                    }
                }
                if (!batch.isEmpty()) {
                    fetchDataBatch(batch);
                }
            }
        };
    }

    private void paginateIndexShard(final ByteBuffer shard, final V value) {
        new Task() {
            @Override
            protected void internalRun() {
                V nextValue = value;
                ColumnList<ByteBuffer> result = null;
                List<K> batch = Lists.newArrayListWithCapacity(keyLimit);

                int pageSize = shardColumnLimit;
                if (pageSize == 0)
                    pageSize = columnLimit;

                do {
                    // Get the first range in the index
                    RangeBuilder range = new RangeBuilder().setStart(Composites.newCompositeBuilder()
                            .greaterThanEquals().addBytes(valSerializer.getNext(valSerializer.toByteBuffer(nextValue)))
                            .build());
                    if (endValue != null) {
                        range.setEnd(Composites.newCompositeBuilder().lessThanEquals().add(endValue, valSerializer)
                                .build());
                    }

                    // Read the index shards
                    try {
                        result = ks.prepareQuery(cfIndex).setConsistencyLevel(consistencyLevel).withRetryPolicy(retry)
                                .getKey(shard).withColumnRange(range.setLimit(pageSize).build()).execute().getResult();
                    }
                    catch (ConnectionException e) {
                        e.printStackTrace();
                        return;
                    }

                    // Read the actual data rows in batches
                    for (Column<ByteBuffer> column : result) {
                        CompositeParser parser = Composites.newCompositeParser(column.getName());
                        nextValue = parser.read(valSerializer);
                        K key = parser.read(cfData.getKeySerializer());

                        if (indexCallback != null) {
                            if (!indexCallback.handleEntry(key, nextValue, column.getByteBufferValue())) {
                                continue;
                            }
                        }

                        if (callback != null) {
                            batch.add(key);

                            if (batch.size() == keyLimit) {
                                fetchDataBatch(batch);
                                batch = Lists.newArrayListWithCapacity(keyLimit);
                            }
                        }
                    }
                } while (result != null && result.size() == pageSize);

                if (!batch.isEmpty()) {
                    fetchDataBatch(batch);
                }
            }
        };
    }

    private void fetchDataBatch(final Collection<K> keys) {
        new Task() {
            @Override
            protected void internalRun() {
                try {
                    OperationResult<Rows<K, C>> result = ks.prepareQuery(cfData).withRetryPolicy(retry)
                            .setConsistencyLevel(consistencyLevel).getKeySlice(keys)
                            .withColumnSlice(new ColumnSlice<C>(columnSlice)).execute();

                    for (Row<K, C> row : result.getResult()) {
                        callback.apply(row);
                    }
                }
                catch (ConnectionException e) {
                    e.printStackTrace();
                }
            }
        };
    }
}

package com.netflix.astyanax.recipes.storage;

import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.collect.Maps;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.consistency.OneOneConsistencyLevelPolicy;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.BoundedExponentialBackoff;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * ChunkProvider responsible for reading and writing chunks to cassandra. Chunks
 * are written to different row keys with the row key name having the format
 * <chunknumber>$<objectname>
 * 
 * @author elandau
 * 
 */
public class CassandraChunkedStorageProvider implements ChunkedStorageProvider {

    private static final RetryPolicy DEFAULT_RETRY_POLICY = new BoundedExponentialBackoff(1000, 10000, 5);
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;
    private static final int DEFAULT_CHUNKSIZE = 0x4000;
    private static final String DEFAULT_ROW_KEY_FORMAT = "%s$%d";

    public enum Columns {
        DATA, OBJECTSIZE, CHUNKSIZE, CHUNKCOUNT, EXPIRES
    }

    private final ColumnFamily<String, String> cf;
    private final Keyspace keyspace;
    private final Map<Columns, String> names = Maps.newHashMap();

    private RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;
    private ConsistencyLevel consistencyLevel = DEFAULT_CONSISTENCY_LEVEL;
    private String rowKeyFormat = DEFAULT_ROW_KEY_FORMAT;

    public CassandraChunkedStorageProvider(Keyspace keyspace, String cfName) {
        this.keyspace = keyspace;
        this.cf = ColumnFamily.newColumnFamily(cfName, StringSerializer.get(), StringSerializer.get());
    }

    public CassandraChunkedStorageProvider(Keyspace keyspace, ColumnFamily<String, String> cf) {
        this.keyspace = keyspace;
        this.cf = cf;
    }

    public CassandraChunkedStorageProvider withColumnName(Columns column, String name) {
        names.put(column, name);
        return this;
    }

    public CassandraChunkedStorageProvider withRowKeyFormat(String format) {
        this.rowKeyFormat = format;
        return this;
    }

    private String getColumnName(Columns column) {
        if (names.containsKey(column))
            return names.get(column);
        return column.name();
    }

    @Override
    public int writeChunk(String objectName, int chunkId, ByteBuffer data, Integer ttl) throws Exception {
        MutationBatch m = keyspace.prepareMutationBatch().withRetryPolicy(retryPolicy);

        m.withRow(cf, getRowKey(objectName, chunkId)).putColumn(getColumnName(Columns.DATA), data, ttl)
                .putColumn(getColumnName(Columns.CHUNKSIZE), data.limit(), ttl);

        if (chunkId == 0) {
            m.withRow(cf, objectName).putColumn(getColumnName(Columns.CHUNKSIZE), data.limit(), ttl);
        }

        m.execute();

        return data.limit();
    }

    @Override
    public ByteBuffer readChunk(String objectName, int chunkId) throws Exception {
        return keyspace.prepareQuery(cf).setConsistencyLevelPolicy(OneOneConsistencyLevelPolicy.get()).withRetryPolicy(retryPolicy)
                .getKey(getRowKey(objectName, chunkId)).getColumn(getColumnName(Columns.DATA)).execute().getResult()
                .getByteBufferValue();
    }

    private String getRowKey(String objectName, int chunkId) {
        return new String(rowKeyFormat).replace("%s", objectName).replace("%d", Integer.toString(chunkId));
    }

    public CassandraChunkedStorageProvider setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return this.consistencyLevel;
    }

    @Override
    public void writeMetadata(String objectName, ObjectMetadata attr) throws Exception {
        MutationBatch m = keyspace.prepareMutationBatch().withRetryPolicy(retryPolicy);

        ColumnListMutation<String> row = m.withRow(cf, objectName);
        if (attr.getChunkSize() != null)
            row.putColumn(getColumnName(Columns.CHUNKSIZE), attr.getChunkSize(), attr.getTtl());
        if (attr.getChunkCount() != null)
            row.putColumn(getColumnName(Columns.CHUNKCOUNT), attr.getChunkCount(), attr.getTtl());
        if (attr.getObjectSize() != null)
            row.putColumn(getColumnName(Columns.OBJECTSIZE), attr.getObjectSize(), attr.getTtl());
        m.execute();
    }

    @Override
    public ObjectMetadata readMetadata(String objectName) throws Exception, NotFoundException {
        ColumnList<String> columns = keyspace.prepareQuery(cf).getKey(objectName).execute().getResult();

        if (columns.isEmpty()) {
            throw new NotFoundException(objectName);
        }

        return new ObjectMetadata().setObjectSize(columns.getLongValue(getColumnName(Columns.OBJECTSIZE), null))
                .setChunkSize(columns.getIntegerValue(getColumnName(Columns.CHUNKSIZE), null))
                .setChunkCount(columns.getIntegerValue(getColumnName(Columns.CHUNKCOUNT), null));
    }

    @Override
    public void deleteObject(String objectName, Integer chunkCount) throws Exception, NotFoundException {
        if (chunkCount == null) {
            ObjectMetadata attr = readMetadata(objectName);
            if (attr.getChunkCount() == null)
                throw new NotFoundException("Object not found :" + objectName);
            chunkCount = attr.getChunkCount();
        }

        MutationBatch m = keyspace.prepareMutationBatch().withRetryPolicy(retryPolicy);

        for (int i = 0; i < chunkCount; i++) {
            m.withRow(cf, getRowKey(objectName, i)).delete();
        }
        m.withRow(cf, objectName).delete();

        m.execute();
    }

    @Override
    public int getDefaultChunkSize() {
        return DEFAULT_CHUNKSIZE;
    }

}

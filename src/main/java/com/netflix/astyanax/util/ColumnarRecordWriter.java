package com.netflix.astyanax.util;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.utils.Pair;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.SerializerPackageImpl;
import com.netflix.astyanax.serializers.UnknownComparatorException;

/**
 * Writer rows where the first pair is the key and subsequent pairs are columns.
 * 
 * @author elandau
 * 
 */
public class ColumnarRecordWriter implements RecordWriter {

    private Keyspace keyspace;
    private SerializerPackage serializers;
    private ColumnFamily<ByteBuffer, ByteBuffer> cf;
    private int batchSize = 1;
    private MutationBatch mutation;

    public ColumnarRecordWriter(Keyspace keyspace, String cfName) {
        this.keyspace = keyspace;
        this.cf = new ColumnFamily<ByteBuffer, ByteBuffer>(cfName, ByteBufferSerializer.get(),
                ByteBufferSerializer.get());
        try {
            this.serializers = keyspace.getSerializerPackage(cfName, true);
        }
        catch (ConnectionException e) {
            this.serializers = SerializerPackageImpl.DEFAULT_SERIALIZER_PACKAGE;
        }
        catch (UnknownComparatorException e) {
            // We should never get this
        }
    }

    public ColumnarRecordWriter(Keyspace keyspace, String cfName, SerializerPackage serializers) {
        this.keyspace = keyspace;
        this.serializers = serializers;
        this.cf = new ColumnFamily<ByteBuffer, ByteBuffer>(cfName, ByteBufferSerializer.get(),
                ByteBufferSerializer.get());
    }

    public ColumnarRecordWriter setBatchSize(int size) {
        this.batchSize = size;
        return this;
    }

    @Override
    public void start() throws ConnectionException {
        this.mutation = keyspace.prepareMutationBatch();
    }

    @Override
    public void write(List<Pair<String, String>> record) {
        if (record.size() <= 1)
            return;

        // Key is first field
        Iterator<Pair<String, String>> iter = record.iterator();
        ByteBuffer rowKey = this.serializers.keyAsByteBuffer(iter.next().right);

        // Build row mutation for all columns
        ColumnListMutation<ByteBuffer> rowMutation = mutation.withRow(cf, rowKey);
        while (iter.hasNext()) {
            Pair<String, String> pair = iter.next();
            try {
                rowMutation.putColumn(
                        this.serializers.columnAsByteBuffer(pair.left),
                        this.serializers.valueAsByteBuffer(pair.left, pair.right), null);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // Execute a mutation
        if (batchSize == mutation.getRowCount()) {
            try {
                mutation.execute();
            }
            catch (ConnectionException e) {
                mutation.discardMutations();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void shutdown() {
        if (mutation.getRowCount() > 0) {
            try {
                mutation.execute();
            }
            catch (ConnectionException e) {
                mutation.discardMutations();
            }
        }
    }
}

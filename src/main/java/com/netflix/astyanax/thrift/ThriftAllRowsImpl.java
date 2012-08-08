package com.netflix.astyanax.thrift;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.KeyRange;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.thrift.model.ThriftColumnOrSuperColumnListImpl;
import com.netflix.astyanax.thrift.model.ThriftRowImpl;

public class ThriftAllRowsImpl<K, C> implements Rows<K, C> {
    private final ColumnFamily<K, C> columnFamily;
    private final AbstractThriftAllRowsQueryImpl<K, C> query;
    private final Supplier<IPartitioner> partitioner;

    public ThriftAllRowsImpl(AbstractThriftAllRowsQueryImpl<K, C> query, ColumnFamily<K, C> columnFamily,
                             Supplier<IPartitioner> partitioner) {
        this.columnFamily = columnFamily;
        this.query = query;
        this.partitioner = partitioner;
    }

    /**
     * Each call to .iterator() returns a new context starting at the beginning
     * of the column family.
     */
    @Override
    public Iterator<Row<K, C>> iterator() {
        return new Iterator<Row<K, C>>() {
            private final IPartitioner partitioner = ThriftAllRowsImpl.this.partitioner.get();
            private final KeyRange range;
            private org.apache.cassandra.thrift.KeySlice lastRow;
            private List<org.apache.cassandra.thrift.KeySlice> list = null;
            private Iterator<org.apache.cassandra.thrift.KeySlice> iter = null;

            {
                // Query from the minimum token to the minimum token.  With the RandomPartitioner the minimum
                // token is -1 which is actually invalid (according to TokenFactory.validate) so use 0 instead.
                String token = partitioner.preservesOrder() ? tokenToString(partitioner.getMinimumToken()) : "0";
                range = new KeyRange().setCount(query.getBlockSize()).setStart_token(token).setEnd_token(token);
            }

            @Override
            public boolean hasNext() {
                // Get the next block
                if (iter == null || (!iter.hasNext() && list.size() == query.getBlockSize())) {
                    if (lastRow != null) {
                        // Determine the start token for the next page
                        Token token = partitioner.getToken(ByteBuffer.wrap(lastRow.getKey()));
                        if (query.getRepeatLastToken() && !partitioner.preservesOrder()) {
                            // Start token is non-inclusive
                            token = new BigIntegerToken(((BigIntegerToken) token).token.subtract(BigInteger.ONE));
                        }
                        range.setStart_token(tokenToString(token));
                    }

                    // Fetch the data
                    list = query.getNextBlock(range);
                    if (list == null) {
                        return false;
                    }
                    iter = list.iterator();
                    if (iter == null || !iter.hasNext()) {
                        return false;
                    }

                    // If repeating last token then skip the first row in the
                    // result
                    if (lastRow != null && query.getRepeatLastToken() && iter.hasNext()) {
                        iter.next();
                    }

                    lastRow = Iterables.getLast(list);
                }
                return iter.hasNext();
            }

            @Override
            public Row<K, C> next() {
                org.apache.cassandra.thrift.KeySlice row = iter.next();
                return new ThriftRowImpl<K, C>(columnFamily.getKeySerializer().fromBytes(row.getKey()),
                        ByteBuffer.wrap(row.getKey()), new ThriftColumnOrSuperColumnListImpl<C>(row.getColumns(),
                                columnFamily.getColumnSerializer()));
            }

            @Override
            public void remove() {
                throw new IllegalStateException();
            }

            private String tokenToString(Token token) {
                return partitioner.getTokenFactory().toString(token);
            }
        };
    }

    @Override
    public Row<K, C> getRow(K key) {
        throw new IllegalStateException();
    }

    @Override
    public int size() {
        throw new IllegalStateException();
    }

    @Override
    public boolean isEmpty() {
        throw new IllegalStateException();
    }

    @Override
    public Row<K, C> getRowByIndex(int i) {
        throw new IllegalStateException();
    }
}

package com.netflix.astyanax.thrift;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;

import com.google.common.collect.Iterables;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.thrift.model.*;

public class ThriftAllRowsImpl<K, C> implements Rows<K, C> {
    private ColumnFamily<K, C> columnFamily;
    private AbstractThriftAllRowsQueryImpl<K, C> query;
    private final RandomPartitioner partitioner = new RandomPartitioner();

    public ThriftAllRowsImpl(AbstractThriftAllRowsQueryImpl<K, C> query, ColumnFamily<K, C> columnFamily) {
        this.columnFamily = columnFamily;
        this.query = query;
    }

    /**
     * Each call to .iterator() returns a new context starting at the beginning
     * of the column family.
     */
    @Override
    public Iterator<Row<K, C>> iterator() {
        return new Iterator<Row<K, C>>() {
            private KeyRange range;
            private org.apache.cassandra.thrift.KeySlice lastRow;
            private List<org.apache.cassandra.thrift.KeySlice> list = null;
            private Iterator<org.apache.cassandra.thrift.KeySlice> iter = null;
            private boolean bContinueSearch = true;
            private boolean bIgnoreTombstones = true;

            {
                range = new KeyRange().setCount(query.getBlockSize()).setStart_token("0").setEnd_token("0");
                
                if (query.getIncludeEmptyRows() == null) {
                    if (query.getPredicate().isSetSlice_range() && query.getPredicate().getSlice_range().getCount() == 0) {
                        bIgnoreTombstones = false;
                    }
                }
                else {
                    bIgnoreTombstones = !query.getIncludeEmptyRows();
                }
            }

            @Override
            public boolean hasNext() {
                // Get the next block
                while (iter == null || (!iter.hasNext() && bContinueSearch)) {
                    if (lastRow != null) {
                        // Determine the start token for the next page
                        String token = partitioner.getToken(ByteBuffer.wrap(lastRow.getKey())).toString();
                        if (query.getRepeatLastToken()) {
                            // Start token is non-inclusive
                            BigInteger intToken = new BigInteger(token).subtract(new BigInteger("1"));
                            range.setStart_token(intToken.toString());
                        }
                        else {
                            range.setStart_token(token);
                        }
                    }

                    // Get the next block of rows from cassandra, exit if none returned
                    list = query.getNextBlock(range);
                    if (list == null) {
                        return false;
                    }
                    
                    // Since we may trim tombstones set a flag indicating whether a complete
                    // block was returned so we can know to try to fetch the next one
                    bContinueSearch = (list.size() == query.getBlockSize());
                    
                    // Trim the list from tombstoned rows, i.e. rows with no columns
                    iter = list.iterator();
                    if (iter == null || !iter.hasNext()) {
                        return false;
                    }
                    
                    org.apache.cassandra.thrift.KeySlice currLastRow = Iterables.getLast(list);
                    
                    if (bIgnoreTombstones) {
                        while (iter.hasNext()) {
                            KeySlice row = iter.next();
                            if (row.getColumns().isEmpty()) {
                                System.out.println("Remove empty row");
                                iter.remove();
                            }
                        }
                    }
                    
                    // If repeating last token then skip the first row in the result
                    if (lastRow != null && query.getRepeatLastToken() && iter.hasNext()) {
                        iter.next();
                    }
                    
                    lastRow = currLastRow;
                    
                    // Get the iterator again
                    iter = list.iterator();
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

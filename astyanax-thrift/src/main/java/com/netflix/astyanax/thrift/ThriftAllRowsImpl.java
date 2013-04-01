/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.thrift;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;

import com.google.common.collect.Iterables;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.thrift.model.*;

public class ThriftAllRowsImpl<K, C> implements Rows<K, C> {
    private ColumnFamily<K, C> columnFamily;
    private ThriftAllRowsQueryImpl<K, C> query;
    private final Partitioner partitioner;

    public ThriftAllRowsImpl(Partitioner partitioner, ThriftAllRowsQueryImpl<K, C> query, ColumnFamily<K, C> columnFamily) {
        this.columnFamily = columnFamily;
        this.query        = query;
        this.partitioner  = partitioner;
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
	            String startToken = query.getStartToken() == null ? partitioner.getMinToken() : query.getStartToken();
	            String endToken = query.getEndToken() == null ? partitioner.getMaxToken() : query.getEndToken();

                range = new KeyRange()
                        .setCount(query.getBlockSize())
                        .setStart_token(startToken)
                        .setEnd_token(endToken);
                
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
                        String token = partitioner.getTokenForKey(ByteBuffer.wrap(lastRow.getKey()));
                        if (query.getRepeatLastToken()) {
                            // Start token is non-inclusive
                            range.setStart_token(partitioner.getTokenMinusOne(token));
                        }
                        else {
                            range.setStart_token(token);
                        }
                    }

                    // Get the next block of rows from cassandra, exit if none returned
                    list = query.getNextBlock(range);
                    if (list == null || list.isEmpty()) {
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

                    KeySlice previousLastRow = lastRow;
                    lastRow = Iterables.getLast(list);
                    
                    if (query.getRepeatLastToken() && previousLastRow != null) {
                        iter.next();
                        iter.remove();
                    }
                    
                    if (iter.hasNext() && bIgnoreTombstones) {
                        // Discard any tombstones
                        while (iter.hasNext()) {
                            KeySlice row = iter.next();
                            if (row.getColumns().isEmpty()) {
                                iter.remove();
                            }
                        }
                        
                        // Get the iterator again
                        iter = list.iterator();
                    }
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

    @Override
    public Collection<K> getKeys() {
        throw new IllegalStateException();
    }
}

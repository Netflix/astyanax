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
package com.netflix.astyanax.thrift.model;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.KeySlice;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

/**
 * Wrapper for a key slice response.  
 * 
 * Will lazily create a lookup by key
 * 
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public class ThriftRowsSliceImpl<K, C> implements Rows<K, C> {

    private List<Row<K,C>>   rows;
    private Map<K, Row<K,C>> lookup;

    public ThriftRowsSliceImpl(List<KeySlice> rows, Serializer<K> keySer, Serializer<C> colSer) {
        this.rows   = Lists.newArrayListWithCapacity(rows.size());
        
        for (KeySlice row : rows) {
            Row<K,C> thriftRow = new ThriftRowImpl<K, C>(
                    keySer.fromBytes(row.getKey()), 
                    ByteBuffer.wrap(row.getKey()),
                    new ThriftColumnOrSuperColumnListImpl<C>(row.getColumns(), colSer));
            this.rows.add(thriftRow);
        }
    }

    @Override
    public Iterator<Row<K, C>> iterator() {
        return rows.iterator();
    }

    @Override
    public Row<K, C> getRow(K key) {
        lazyBuildLookup();
        return lookup.get(key);
    }

    @Override
    public int size() {
        return rows.size();
    }

    @Override
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    @Override
    public Row<K, C> getRowByIndex(int index) {
        return rows.get(index);
    }

    @Override
    public Collection<K> getKeys() {
        return Lists.transform(rows, new Function<Row<K,C>, K>() {
            @Override
            public K apply(Row<K, C> row) {
                return row.getKey();
            }
        });
    }
    
    private void lazyBuildLookup() {
        if (lookup == null) {
            this.lookup = Maps.newHashMap();
            for (Row<K,C> row : rows) {
                this.lookup.put(row.getKey(),  row);
            }
        }
    }
}

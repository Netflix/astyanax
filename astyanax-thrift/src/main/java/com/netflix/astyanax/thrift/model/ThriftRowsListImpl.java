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
import java.util.Map.Entry;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class ThriftRowsListImpl<K, C> implements Rows<K, C> {
    private List<Row<K, C>>   rows;
    private Map<K, Row<K, C>> lookup;

    public ThriftRowsListImpl(Map<ByteBuffer, List<ColumnOrSuperColumn>> rows, Serializer<K> keySer, Serializer<C> colSer) {
        this.rows   = Lists.newArrayListWithCapacity(rows.size());
        this.lookup = Maps.newLinkedHashMap();
        
        for (Entry<ByteBuffer, List<ColumnOrSuperColumn>> row : rows.entrySet()) {
            Row<K,C> thriftRow = new ThriftRowImpl<K, C>(
                    keySer.fromByteBuffer(row.getKey().duplicate()), 
                    row.getKey(),
                    new ThriftColumnOrSuperColumnListImpl<C>(row.getValue(), colSer));

            this.rows.add(thriftRow);
            lookup.put(thriftRow.getKey(), thriftRow);
        }
    }

    @Override
    public Iterator<Row<K, C>> iterator() {
        return rows.iterator();
    }

    @Override
    public Row<K, C> getRow(K key) {
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
    public Row<K, C> getRowByIndex(int i) {
        return rows.get(i);
    }

    @Override
    public Collection<K> getKeys() {
        return lookup.keySet();
    }

}

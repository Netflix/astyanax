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
import org.apache.cassandra.thrift.CqlRow;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class ThriftCqlRowsImpl<K, C> implements Rows<K, C> {
    private List<Row<K, C>> rows;
    private Map<K, Row<K, C>> lookup;

    public ThriftCqlRowsImpl(final List<CqlRow> rows,
            final Serializer<K> keySer, final Serializer<C> colSer) {
        this.rows = Lists.newArrayListWithCapacity(rows.size());
        for (CqlRow row : rows) {
            byte[] keyBytes = row.getKey();
            if (keyBytes == null || keyBytes.length == 0) {
                this.rows.add(new ThriftRowImpl<K, C>(null, null,
                        new ThriftColumnListImpl<C>(row.getColumns(), colSer)));
            } else {
                this.rows.add(new ThriftRowImpl<K, C>(keySer
                        .fromBytes(keyBytes), ByteBuffer.wrap(keyBytes),
                        new ThriftColumnListImpl<C>(row.getColumns(), colSer)));
            }
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
        return this.rows.size();
    }

    @Override
    public boolean isEmpty() {
        return this.rows.isEmpty();
    }

    @Override
    public Row<K, C> getRowByIndex(int i) {
        return rows.get(i);
    }

    @Override
    public Collection<K> getKeys() {
        return Lists.transform(rows, new Function<Row<K, C>, K>() {
            @Override
            public K apply(Row<K, C> input) {
                return input.getKey();
            }
        });
    }

    private void lazyBuildLookup() {
        if (lookup == null) {
            this.lookup = Maps.newHashMap();
            for (Row<K, C> row : rows) {
                this.lookup.put(row.getKey(), row);
            }
        }
    }

}

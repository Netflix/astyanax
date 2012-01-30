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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.KeySlice;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class ThriftRowsSliceImpl<K,C> implements Rows<K, C> {

	private List<KeySlice> rows;
	private Map<K, List<ColumnOrSuperColumn>> lookup;
	private Serializer<K> keySer;
	private Serializer<C> colSer;
	
	public ThriftRowsSliceImpl(List<KeySlice> rows, Serializer<K> keySer, Serializer<C> colSer) {
		this.keySer = keySer;
		this.colSer = colSer;
		this.rows = rows;
	}
	
	@Override
	public Iterator<Row<K, C>> iterator() {
		class IteratorImpl implements Iterator<Row<K, C>> {
			Iterator<KeySlice> base;
			
			public IteratorImpl(Iterator<KeySlice> base) {
				this.base = base;
			}
			
			@Override
			public boolean hasNext() {
				return base.hasNext();
			}

			@Override
			public Row<K, C> next() {
				KeySlice ks = base.next();
				return new ThriftRowImpl<K,C>(keySer.fromBytes(ks.getKey()), ByteBuffer.wrap(ks.getKey()),
								   new ThriftColumnOrSuperColumnListImpl<C>(ks.getColumns(), colSer));
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Iterator is immutable");
			}
			
		}
		return new IteratorImpl(rows.iterator());
	}

	@Override
	public Row<K, C> getRow(K key) {
		if (lookup == null) {
			lookup = new HashMap<K, List<ColumnOrSuperColumn>>(rows.size());
			for (KeySlice slice : rows) {
				lookup.put(keySer.fromBytes(slice.getKey()), slice.getColumns());
			}
		}
		
		List<ColumnOrSuperColumn> columns = lookup.get(key);
		if (key == null) {
			return null;
		}
		return new ThriftRowImpl<K,C>(key, keySer.toByteBuffer(key),
				   new ThriftColumnOrSuperColumnListImpl<C>(columns, colSer));
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
    public Row<K,C> getRowByIndex(int index) {
        KeySlice ks = rows.get(index);
        return new ThriftRowImpl<K,C>(keySer.fromBytes(ks.getKey()), ByteBuffer.wrap(ks.getKey()),
                new ThriftColumnOrSuperColumnListImpl<C>(ks.getColumns(), colSer));
    }
}

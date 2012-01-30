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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class ThriftRowsListImpl<K,C> implements Rows<K,C> {

	private Map<ByteBuffer, List<ColumnOrSuperColumn>> rows;
	private final Serializer<K> keySer;
	private final Serializer<C> colSer;
	
	public ThriftRowsListImpl(Map<ByteBuffer, List<ColumnOrSuperColumn>> rows, 
			Serializer<K> keySer, Serializer<C> colSer) {
		this.rows = rows;
		this.keySer = keySer;
		this.colSer = colSer;
	}
	
	@Override
	public Iterator<Row<K, C>> iterator() {
		class IteratorImpl implements Iterator<Row<K, C>> {
			Iterator<Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>>> base;
			
			public IteratorImpl(Iterator<Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>>> base) {
				this.base = base;
			}
			
			@Override
			public boolean hasNext() {
				return base.hasNext();
			}

			@Override
			public Row<K, C> next() {
				Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> row = base.next();
				return new ThriftRowImpl<K,C>(keySer.fromByteBuffer(row.getKey().duplicate()), row.getKey(),
								   new ThriftColumnOrSuperColumnListImpl<C>(row.getValue(), colSer));
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Iterator is immutable");
			}
			
		}
		return new IteratorImpl(rows.entrySet().iterator());	
	}

	@Override
	public Row<K, C> getRow(K key) {
		List<ColumnOrSuperColumn> columns = rows.get(keySer.toByteBuffer(key));
		if (columns == null) {
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
    public Row<K, C> getRowByIndex(int i) {
        throw new UnsupportedOperationException("RowSlice response is not sorted");
    }

}

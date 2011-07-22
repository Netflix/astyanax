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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlRow;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class ThriftCqlRowsImpl<K,C> implements Rows<K,C> {
	private final List<CqlRow> rows;
	private Map<K, List<Column>> lookup;
	private final Serializer<K> keySer;
	private final Serializer<C> colSer;
	
	public ThriftCqlRowsImpl(List<CqlRow> rows, Serializer<K> keySer, Serializer<C> colSer) {
		this.rows = rows;
		this.keySer = keySer;
		this.colSer = colSer;
	}
	
	@Override
	public Iterator<Row<K, C>> iterator() {
		class IteratorImpl implements Iterator<Row<K, C>> {
			Iterator<CqlRow> base;
			
			public IteratorImpl(Iterator<CqlRow> base) {
				this.base = base;
			}
			
			@Override
			public boolean hasNext() {
				return base.hasNext();
			}

			@Override
			public Row<K, C> next() {
				CqlRow row = base.next();
				return new ThriftRowImpl<K,C>(keySer.fromBytes(row.getKey()),
								   new ThriftColumnListImpl<C>(row.getColumns(), colSer));
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Iterator is immutable");
			}
			
		}
		return new IteratorImpl(rows.iterator());	}

	@Override
	public Row<K, C> getRow(K key) {
		if (lookup == null) {
			lookup = new HashMap<K, List<Column>>(rows.size());
			for (CqlRow row : rows) {
				lookup.put(keySer.fromBytes(row.getKey()), row.getColumns());
			}
		}
		
		List<Column> columns = lookup.get(key);
		if (key == null) {
			// TODO: Shouldn't this throw a not found exception
			return null;
		}
		return new ThriftRowImpl<K,C>(key,
				   new ThriftColumnListImpl<C>(columns, colSer));
	}

	@Override
	public int size() {
		return this.rows.size();
	}

	@Override
	public boolean isEmpty() {
		return this.rows.isEmpty();
	}

}

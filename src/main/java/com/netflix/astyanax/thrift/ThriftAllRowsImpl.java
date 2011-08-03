package com.netflix.astyanax.thrift;

import java.util.Iterator;
import java.util.List;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class ThriftAllRowsImpl<K,C> implements Rows<K, C> {
	private ColumnFamily<K, C> columnFamily;
	private List<org.apache.cassandra.thrift.KeySlice> list = null;
	private Iterator<org.apache.cassandra.thrift.KeySlice> iter = null;
	private boolean repeatLastToken = true;
	private AbstractThriftAllRowsQueryImpl<K,C> query;

	public ThriftAllRowsImpl(AbstractThriftAllRowsQueryImpl<K,C> query, 
				ColumnFamily<K,C> columnFamily,
				List<org.apache.cassandra.thrift.KeySlice> list) {
		this.columnFamily = columnFamily;
		this.query = query;
		this.list = list;
		this.iter = list.iterator();
	}
	
	@Override
	public Iterator<Row<K, C>> iterator() {
		return new Iterator<Row<K,C>>() {
			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}

			@Override
			public Row<K, C> next() {
				org.apache.cassandra.thrift.KeySlice row = iter.next();
				// Get the next block
				if (!iter.hasNext() && list.size() == query.getBlockSize()) {
					query.setLastRowKey(row.bufferForKey());
					
					// Fetch the data
					try {
						list = query.getNextBlock();
					} catch (ConnectionException e) {
						throw new RuntimeException(e);
					}
					iter = list.iterator();
					
					// If repeating last token then skip the first row in the result
					if (repeatLastToken && iter.hasNext()) {
						iter.next();
					}
				}
				return new ThriftRowImpl<K,C>(columnFamily.getKeySerializer().fromBytes(row.getKey()),
								   new ThriftColumnOrSuperColumnListImpl<C>(row.getColumns(), columnFamily.getColumnSerializer()));
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
}

package com.netflix.astyanax.index;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowSliceColumnCountQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.SerializerTypeInferer;

/**
 * The problem with the approach is that we have to do double iteration 
 * of the rows returned from a {@link RowSliceQuery}.
 * 
 * The trade off is that the number of rows is low (it's a high cardinality index)
 * 
 * @author marcus
 *
 * @param <K>
 * @param <C>
 * @param <V>
 */
public class HCIndexQueryImpl<K, C, V> implements HighCardinalityQuery<K, C, V> {

	private Keyspace keyspace;
	private ColumnFamily<K, C> columnFamily;
	
	private IndexCoordination indexCoordination = null;
	
	public HCIndexQueryImpl(Keyspace keyspace,ColumnFamily<K, C> columnFamily) {
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;
		
		indexCoordination = IndexCoordinationFactory.getIndexContext();
	}
	

	@Override
	public RowSliceQuery<K, C> equals(C name, V value) {
		//OK, this is where it happens
		ColumnFamilyQuery<K, C> query = keyspace.prepareQuery(columnFamily);
		
		Index<C, V, K> ind = new IndexImpl<C, V, K>(keyspace,columnFamily.getName());
		try {
			//get keys associated with 
			Collection<K> keys = ind.eq(name, value);
			
			RowSliceQuery<K,C> rsqImpl = query.getRowSlice(keys);
			
			RowSliceQueryWrapper wrapper = new RowSliceQueryWrapper(rsqImpl,indexCoordination,columnFamily);
			
			
			return wrapper;
			
		}catch (ConnectionException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getCause());
		}
		
	}
	
	public Keyspace getKeyspace() {
		return keyspace;
	}


	public void setKeyspace(Keyspace keyspace) {
		this.keyspace = keyspace;
	}


	public ColumnFamily<K, C> getColumnFamily() {
		return columnFamily;
	}


	public void setColumnFamily(ColumnFamily<K, C> columnFamily) {
		this.columnFamily = columnFamily;
	}


	public IndexCoordination getIndexCoordination() {
		return indexCoordination;
	}


	public void setIndexCoordination(IndexCoordination indexCoordination) {
		this.indexCoordination = indexCoordination;
	}

	/**
	 * 
	 * @author marcus
	 *
	 */
	class RowSliceQueryWrapper implements RowSliceQuery<K, C> {

		RowSliceQuery<K, C> impl;
		IndexCoordination indexContext;
		ColumnFamily<K, C> cf;
		HashMap<C,IndexMappingKey<C>> colsMapped = new HashMap<C,IndexMappingKey<C>>();
		boolean columnsSelected  = false;
		
		RowSliceQueryWrapper(RowSliceQuery<K, C> impl,IndexCoordination indexContext,ColumnFamily<K, C> cf) {
			this.impl = impl;
			this.indexContext = indexContext;
			this.cf = cf;
			//this.colsMapped = indexContext.getMetaDataByCf(cf.getName());
		}
		@Override
		public OperationResult<Rows<K, C>> execute() throws ConnectionException {
			
			//here we'll wrap the result
			//and identify the columns that returned that
			//are indexed
			return onExecute(  impl.execute() );
			
		}
		
		private OperationResult<Rows<K,C>> onExecute(OperationResult<Rows<K,C>> opResult) {
			
			Iterator<Row<K,C>> iter =  opResult.getResult().iterator();
			
			//we don't have a column slice selected
			//we'll have to check for all of them.
			if (!columnsSelected) {
				
				List<IndexMetadata<C, K>> list = indexContext.getMetaDataByCf(cf.getName());
				for (IndexMetadata<C, K> metadata:list) {
					colsMapped.put(metadata.getIndexKey().getColumnName(), metadata.getIndexKey());
				}
			}
			//This is an iteration over all the rows returned
			//however if this is truly high cardinality, it will be a small number
			
			while (iter.hasNext()) {
				Row<K,C> row = iter.next();
				
				for (C col: colsMapped.keySet()) {
					Column<C> column = row.getColumns().getColumnByName(col);
					
					//I don't have to read this
					if (column == null)
						continue;
					//we don't know the value type - get it from meta data
					byte[] b = column.getByteArrayValue();
					IndexMappingKey<C> mappingKey = colsMapped.get(col);
					IndexMetadata<C,K> md = indexContext.getMetaData(mappingKey);
					Serializer<K> serializer = SerializerTypeInferer.getSerializer(md.getRowKeyClass());
					
					//TODO: catch the no meta data exception??
					//possible warn or throw an exception
					K colVal = serializer.fromBytes(b);
					indexContext.reading(new IndexMapping<C,K>(mappingKey,colVal,colVal));
					
					
				}
					
				
				
			}
			
			return opResult;
		}
		
		
		@Override
		public Future<OperationResult<Rows<K, C>>> executeAsync()
				throws ConnectionException {
			//not supported at this time
			//we won't support this until we move away from 
			//a thread local implementation
			return impl.executeAsync();
			
		}
		private void onAddedColumns(C...columns) {
			columnsSelected = true;
			for (C column:columns) {
				if (indexContext.indexExists(cf.getName(), column)) {
					colsMapped.put(column,new IndexMappingKey<C>(cf.getName(), column));
				}
			}
		}
		@Override
		public RowSliceQuery<K, C> withColumnSlice(C... columns) {
			onAddedColumns(columns);
			return impl.withColumnSlice(columns);
		}

		@Override
		public RowSliceQuery<K, C> withColumnSlice(Collection<C> columns) {
			onAddedColumns((C[])columns.toArray());
			return impl.withColumnSlice(columns);
		}

		@Override
		public RowSliceQuery<K, C> withColumnSlice(ColumnSlice<C> columns) {
			
			return withColumnRange(columns.getStartColumn(), columns.getEndColumn(), columns.getReversed(),columns.getLimit());
		}

		@Override
		public RowSliceQuery<K, C> withColumnRange(C startColumn, C endColumn,
				boolean reversed, int count) {
			
			//not supported, we don't know if it falls in this range.
			
			return impl.withColumnRange(startColumn,endColumn,reversed,count);
		}

		@Override
		public RowSliceQuery<K, C> withColumnRange(ByteBuffer startColumn,
				ByteBuffer endColumn, boolean reversed, int count) {
			return impl.withColumnRange(startColumn,endColumn,reversed,count);
		}

		@Override
		public RowSliceQuery<K, C> withColumnRange(ByteBufferRange range) {
			// TODO Auto-generated method stub
			return impl.withColumnRange(range);
		}

		@Override
		public RowSliceColumnCountQuery<K> getColumnCounts() {
			return impl.getColumnCounts();
		}
		
	}

}

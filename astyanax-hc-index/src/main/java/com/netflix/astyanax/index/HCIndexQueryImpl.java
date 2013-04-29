/* 
 * Copyright (c) 2013 Research In Motion Limited. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 */
package com.netflix.astyanax.index;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
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
import com.netflix.astyanax.serializers.TypeInferringSerializer;

/**
 * Implementation of {@link HighCardinalityQuery}.
 * 
 * It also returns a wrapped implementation of the {@link RowSliceQuery}, which would be the logical
 * return type from a row slice or indexed slice query.
 * 
 * 2 interaction points
 * <ul>
 * <li> {@link Index} - reads from the index based on {@link #equals(Object, Object)} operation
 * performed by client
 * <li> {@link IndexCoordination} - puts reads in transient memory for potential downstream puts
 * </ul>
 * 
 * The problem with the approach of detecting changes in column index modification
 * is that we have to do double iteration 
 * of the rows returned from a {@link RowSliceQuery}.
 *  * The trade off is that the number of rows is low (it's a high cardinality index)
 * 
 * 
 * 
 * @author marcus
 *
 * @param <K>
 * @param <C>
 * @param <V>
 */
public class HCIndexQueryImpl<K, C, V> implements HighCardinalityQuery<K, C, V> {

	private static Logger log = LoggerFactory.getLogger(HCIndexQueryImpl.class);
	
	private Keyspace keyspace;
	private ColumnFamily<K, C> columnFamily;
	
	private IndexCoordination indexCoordination = null;
	
	private RepairListener<K, C, V> repairListener;
	
	/**
	 * Repair executor.
	 * 
	 */
	private Executor repairExecutor;
	
	public HCIndexQueryImpl(Keyspace keyspace,ColumnFamily<K, C> columnFamily) {
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;
		
		indexCoordination = IndexCoordinationFactory.getIndexContext();
		
		repairExecutor = Executors.newFixedThreadPool(1);
	}
	
	public HCIndexQueryImpl(Keyspace keyspace,ColumnFamily<K, C> columnFamily,RepairListener<K, C, V> repairListener) {
		this(keyspace,columnFamily);
		registerRepairListener(repairListener);
	}
	/**
	 * Not yet implemented.  Have to think of the what executor will be used
	 * for the notification
	 */
	@Override
	public void registerRepairListener(RepairListener<K, C, V> repairListener) {
		this.repairListener = repairListener;
		
	}

	@Override
	public RowSliceQuery<K, C> equals(C name, V value) {
		//OK, this is where it happens
		ColumnFamilyQuery<K, C> query = keyspace.prepareQuery(columnFamily);
		IndexMetadata<C, K> metaData = indexCoordination.getMetaData(new IndexMappingKey<C>(columnFamily.getName(),name));
		Index<C, V, K> ind = null;
		if (metaData != null)
			ind = new IndexImpl<C, V, K>(keyspace,columnFamily.getName(),metaData.getIndexCFName());
		else
			ind = new IndexImpl<C, V, K>(keyspace,columnFamily.getName(),IndexImpl.DEFAULT_INDEX_CF);
		
		try {
			//get keys for this reverse index
			Collection<K> keys = ind.eq(name, value);
			//The real row slice query
			RowSliceQuery<K,C> rsqImpl = query.getRowSlice(keys);
			//a wrapped version of the real row slice query
			RowSliceQueryWrapper wrapper = new RowSliceQueryWrapper(rsqImpl,indexCoordination,columnFamily,name,value,repairListener);
			
			
			return wrapper;
			
		}catch (ConnectionException e) {
			log.error(e.getMessage(),e);
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

		//private static Logger log = LoggerFactory.getLogger(RowSliceQueryWrapper.class);
		RowSliceQuery<K, C> impl;
		IndexCoordination indexContext;
		ColumnFamily<K, C> cf;
		Map<C,IndexMappingKey<C>> colsMapped = null;
		boolean columnsSelected  = false;
		C colEq;
		//ByteBuffer buffColVal;
		V colVal;
		RepairListener<K, C, V> repairListener;
		
		RowSliceQueryWrapper(RowSliceQuery<K, C> impl,IndexCoordination indexContext,ColumnFamily<K, C> cf,C colEQ, V colVal,RepairListener<K, C, V> repair) {
			this.impl = impl;
			this.indexContext = indexContext;
			this.cf = cf;
			this.colsMapped = indexContext.getColumnsMapped(cf.getName());
			this.colEq = colEQ;
			//this.buffColVal = TypeInferringSerializer.get().toByteBuffer(colVal);
			this.colVal = colVal;
			this.repairListener = repair;
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
			
			//no index meta data - just return the result
			if (colsMapped == null)
				return opResult;
			
			//This is an iteration over all the rows returned
			//however if this is truly high cardinality, it will be a small number
			//why do I need to do this?
			//this is required because I need to understand what index is being "read"
			//so that an appropriate update can take place down stream
			//if the client does indeed modify the index value
			//and we can remove the old value of the index.
			while (iter.hasNext()) {
				Row<K,C> row = iter.next();
				
				
				//first determine if we need to drop this from the iterator:
				for (C col: colsMapped.keySet()) {
					Column<C> column = row.getColumns().getColumnByName(col);
					
					//I don't have to read this
					if (column == null)
						continue;
					
					IndexMappingKey<C> mappingKey = colsMapped.get(col);
					
					Serializer<V> colSerializer = SerializerTypeInferer.getSerializer(colVal.getClass());
					V newColVal = column.getValue(colSerializer);
					
					IndexMapping<C,V> indMap = new IndexMapping<C,V>(mappingKey,newColVal,colVal);
					indexContext.reading(indMap);
					
					//invoke repair
					//if the colEq and being compared
					//And the column value returned this query doesn't
					//match the one one from the index
					if (colEq.equals(col) &&
							!colVal.equals(newColVal)) {
						iter.remove();
						
						repair(row.getKey(),indMap,row.getKey());
					}
					
					
				}
					
				
				
			}
			
			return opResult;
		}
		
		private void repair(K pkValue,final IndexMapping<C, V> mapping,final K key) {
			MutationBatch repairBatch = keyspace.prepareMutationBatch();
			IndexImpl<C, V, K> ind = new IndexImpl<C, V, K>(keyspace,repairBatch , cf.getName());
			
			ind.updateIndex(colEq, colVal, mapping.getOldValueofCol(),pkValue);
			
			try {
				ListenableFuture<OperationResult<Void>> future = repairBatch.executeAsync();
				final RepairListener<K, C, V> rl = this.repairListener;
				if (rl != null)
					future.addListener(new Runnable() {
						//
						public void run() {
							
							rl.onRepair(mapping, key);
							
						}
					
					
					}, repairExecutor);
				
			} catch (ConnectionException e) {
				log.error("Error repairing index cf " + cf.getName() + " rowkey= " + pkValue, e);
			}
		}
		
		
		@Override
		public ListenableFuture<OperationResult<Rows<K, C>>> executeAsync()
				throws ConnectionException {
			//not supported at this time
			//we won't support this until we move away from 
			//a thread local implementation
			return impl.executeAsync();
			
		}
		private void onAddedColumns(C...columns) {
			columnsSelected = true;
			
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

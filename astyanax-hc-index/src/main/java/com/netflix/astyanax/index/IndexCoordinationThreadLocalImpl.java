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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.SerializerTypeInferer;

/**
 * The point of synchronization between the clients  
 * 
 * supports a synchronous get and put model nicely, however
 * asynchronous is not supported in this implementation.
 * 
 * It also supports <strong>a write once only</strong>, as the update to index 
 * will remove the value from the thread local.
 * 
 *  
 * TODO: provide other non-thread local implementations (one that is a fixed length static
 * hashmap) this is also a reasonable approach.
 * 
 * @author marcus
 *
 */
public class IndexCoordinationThreadLocalImpl implements IndexCoordination {

	
	private static class IndexContextLocal extends ThreadLocal<Map<IndexMappingKey<?>,IndexMapping<?,?>>> {

		@Override
		protected Map<IndexMappingKey<?>, IndexMapping<?, ?>> initialValue() {
			return new HashMap<IndexMappingKey<?>, IndexMapping<?,?>>();
		}
		
		
	}
	
	private static final IndexContextLocal indexMapLocal = new IndexContextLocal();
	
	
	//static data
	//we need to "prime" this first because 
	//reads need to know what values needed to cache
	//clients will provide this.
	private Map<IndexMappingKey<?>,IndexMetadata<?,?>> metaDataSet;
	private Map<String,ArrayList<IndexMetadata<?, ?>>> metaDataByCF;
	
	/**
	 * 
	 * sorry, going to avoid all generics typing - it's not constructive at this
	 * point.
	 */
	
	private Map<Object,Object> cfToColsMapped = new HashMap<Object, Object>();
	
	
	public IndexCoordinationThreadLocalImpl() {
		metaDataSet = new HashMap<IndexMappingKey<?>,IndexMetadata<?,?>>();
		metaDataByCF = new HashMap<String, ArrayList<IndexMetadata<?,?>>>();
	}
	
	
	
	@Override
	public <C, V, K> Index<C, V, K> getIndex(IndexMetadata<C, K> metaData,
			Keyspace keyspace, MutationBatch mutation) {
		
		return new IndexImpl<C, V, K>(keyspace, mutation, metaData.getIndexKey().getColumnFamily(), metaData.getIndexCFName());
	}



	@Override
	public <C, V, K> Index<C, V, K> getReadIndex(IndexMetadata<C, K> metaData,
			Keyspace keyspace) {
		return new IndexImpl<C, V, K>(keyspace,  metaData.getIndexKey().getColumnFamily(), metaData.getIndexCFName());
	}



	/**
	 * Adding the index meta data.
	 * 
	 *  
	 */
 	@Override
	public <C,K> void addIndexMetaData(IndexMetadata<C,K> metaData) {
		
 		metaDataSet.put(metaData.getIndexKey(),metaData);
 		
 		ArrayList<IndexMetadata<?, ?>> list = metaDataByCF.get(metaData.getIndexKey().getColumnFamily());
 		if (metaDataByCF.get(metaData.getIndexKey().getColumnFamily()) ==null) {
 			list = new ArrayList<IndexMetadata<?,?>>();
 			metaDataByCF.put(metaData.getIndexKey().getColumnFamily(),list );
 		}
 		list.add(metaData);
 		
 		//add cf to column information
 		HashMap<C,IndexMappingKey<C>> colsMapped = new HashMap<C,IndexMappingKey<C>>();
		
		List<IndexMetadata<C, K>> cfList = getMetaDataByCf(metaData.getIndexKey().getColumnFamily());
		if (cfList != null) {
			for (IndexMetadata<C, K> metadata:cfList) {
				colsMapped.put(metadata.getIndexKey().getColumnName(), metadata.getIndexKey());
			}
			
			cfToColsMapped.put(metaData.getIndexKey().getColumnFamily(), colsMapped);
		}
		
		
	}
 	
	@Override
	public <C, K> void addIndexMetaDataAndSchema(Keyspace keyspace,IndexMetadata<C, K> metaData) throws ConnectionException {
		
		if (metaData.getIndexCFOptions() == null)
			SchemaIndexUtil.createIndexCF(keyspace, metaData.getIndexCFName(), false, true);
		else 
			SchemaIndexUtil.createIndexCF(keyspace, metaData.getIndexCFName(), false,metaData.getIndexCFOptions());
		
		addIndexMetaData(metaData);
	}
	@Override
	public <C, K> IndexMetadata<C, K> getMetaData(IndexMappingKey<C> key) {
		return (IndexMetadata<C,K>)metaDataSet.get(key);
	}

	public <C,K> List<IndexMetadata<C, K>> getMetaDataByCf(String cf) {
		
		ArrayList<IndexMetadata<?, ?>> uncastlist = metaDataByCF.get(cf);
		if (uncastlist == null)
			return null;
		ArrayList<IndexMetadata<C,K>> castList = new ArrayList<IndexMetadata<C,K>>();
		for (IndexMetadata<?, ?> indexdata:uncastlist) {
			castList.add((IndexMetadata<C,K>)indexdata);
		}
		return castList;
		
	}
	@Override
	public <C> boolean indexExists(IndexMappingKey<C> key) {
		return metaDataSet.get(key) != null;
	}

	
	@Override
	public <C> boolean indexExists(String cf, C columnName) {
		return indexExists(new IndexMappingKey<C>(cf, columnName));
	}

	@Override
	public <C, V> void reading(IndexMapping<C, V> mapping) throws NoMetaDataException {
		
		IndexMappingKey<C> key = mapping.getColKey();
		
		if (!indexExists(key))
			throw new NoMetaDataException();
		
		indexMapLocal.get().put(key,mapping);
		
	}
	
	
	@Override
	public <C, V> IndexMapping<C, V> modifying(IndexMappingKey<C> key, V newValue)
			throws NoReadException {
		
		IndexMapping<C,V> mapping = (IndexMapping<C,V>)indexMapLocal.get().get(key);
		
		//2 possiblilities
		//that we haven't read through here using reading method
		//that it's new value - we'll say its the second case
		//otherwise it's a user bug.
		if (mapping == null) {
			//throw new NoReadException();
			//assume new "insert"
			mapping = new IndexMapping<C, V>(key,newValue);
			indexMapLocal.get().put(key, mapping);
			
		}
		
		mapping.setValueOfCol(newValue);
		
		return mapping;
		
	}
	
	//Gets and possibly modifies it for future updates.
	//queries the data on behalf
	/**
	 * Gets while caching result, for potentional put downstream.
	 * 
	 * @param rowKey
	 * @param keyspace
	 * @param cf
	 * @return
	 * @throws ConnectionException
	 */
	public <K,C> OperationResult<ColumnList<C>>  reading(K rowKey,Keyspace keyspace,ColumnFamily<K,C> cf) throws ConnectionException {
		
		
		RowQuery<K,C> rq = keyspace.prepareQuery(cf).getKey(rowKey);
		OperationResult<ColumnList<C>> ores = rq.execute();
		ColumnList<C> colResult = ores.getResult();
		reading(colResult, cf);
		
		return ores;
		
	}
	
	
	public <K,C> OperationResult<Rows<K,C>>  reading(Collection<K> keys,Keyspace keyspace,ColumnFamily<K,C> cf) throws ConnectionException {
		
		OperationResult<Rows<K,C>> rowsRes = keyspace.prepareQuery(cf).getKeySlice(keys).execute();
		
		for (Row<K,C> row:rowsRes.getResult()) {
			reading(row.getColumns(), cf);
		}
		
		return rowsRes;
		
	}

	private <K,C>void reading (ColumnList<C> columnList,ColumnFamily<K, C> cf) {
		
		@SuppressWarnings("unchecked")
		HashMap<C,IndexMappingKey<C>> colsMapped = (HashMap<C,IndexMappingKey<C>>)cfToColsMapped.get(cf.getName());
		if (colsMapped == null)
			return;
		for (C col: colsMapped.keySet()) {
			
			
			Column<C> column = columnList.getColumnByName(col);
			if (column == null)
				continue;
			IndexMappingKey<C> mappingKey = colsMapped.get(col);
			IndexMetadata<C,K> md = getMetaData(mappingKey);
			Serializer<K> serializer = SerializerTypeInferer.getSerializer(md.getRowKeyClass());
			
			byte[] b = column.getByteArrayValue();
			K colVal = serializer.fromBytes(b);
			reading(new IndexMapping<C,K>(mappingKey,colVal,colVal));
		}
	}
	

	@Override
	public <C, V> IndexMapping<C, V> get(IndexMappingKey<C> key) {
		
		
		IndexMapping<C,V> indexMapping = (IndexMapping<C,V>)indexMapLocal.get().get(key);
		if (indexMapping == null) {
			indexMapping = new IndexMapping<C, V>(key,null);
			indexMapLocal.get().put(key, indexMapping);
		}
		
		return indexMapping;
		
	}

	
	@SuppressWarnings("unchecked")
	public <C> Map<C,IndexMappingKey<C>> getColumnsMapped(ColumnFamily<?, C> cf) {
		return (HashMap<C,IndexMappingKey<C>>)cfToColsMapped.get(cf.getName());
		
		
	}
	@SuppressWarnings("unchecked")
	public <C> Map<C,IndexMappingKey<C>> getColumnsMapped(String cf) {
		return (HashMap<C,IndexMappingKey<C>>)cfToColsMapped.get(cf);
		
		
	}
	

}

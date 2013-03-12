package com.netflix.astyanax.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

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
	
	
	public IndexCoordinationThreadLocalImpl() {
		metaDataSet = new HashMap<IndexMappingKey<?>,IndexMetadata<?,?>>();
		metaDataByCF = new HashMap<String, ArrayList<IndexMetadata<?,?>>>();
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
 		
 		//
		
	}
 	
	@Override
	public <C, K> void addIndexMetaDataAndSchema(Keyspace keyspace,IndexMetadata<C, K> metaData) throws ConnectionException {
		
		if (metaData.getIndexCFOptions() == null)
			SchemaIndexUtil.createIndexCF(keyspace, metaData.getIndexCFName(), true, true);
		else 
			SchemaIndexUtil.createIndexCF(keyspace, metaData.getIndexCFName(), true,metaData.getIndexCFOptions());
		
		addIndexMetaData(metaData);
	}
	@Override
	public <C, K> IndexMetadata<C, K> getMetaData(IndexMappingKey<C> key) {
		return (IndexMetadata<C,K>)metaDataSet.get(key);
	}

	public <C,K> List<IndexMetadata<C, K>> getMetaDataByCf(String cf) {
		
		ArrayList<IndexMetadata<?, ?>> uncastlist = metaDataByCF.get(cf);
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
	public <C, V> void reading(String cf, C columnName, V value)
			throws NoMetaDataException {
		
		reading (new IndexMapping<C, V>(cf, columnName, value, value));
		
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

	@Override
	public <C, V> IndexMapping<C,V> modifying(String cf, C columnName, V newValue)
			throws NoReadException {
		
		return modifying(new IndexMappingKey(cf, columnName), newValue);
		
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

	
	

}

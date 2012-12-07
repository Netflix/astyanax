package com.netflix.astyanax.index;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.netflix.astyanax.index.IndexCoordination.NoMetaDataException;
import com.netflix.astyanax.index.IndexCoordination.NoReadException;

/**
 * The point of synchronization between the clients  
 * 
 * supports a synchronous get and put model nicely, however
 * asynchronous is not supported in this implementation.
 * 
 * It also supports <strong>a write once only</strong>, as the update to index 
 * will remove the value from the thread local.
 * 
 * TODO: tie into transaction context.
 * 
 * TODO: provide other non-thread local implementations (one that is a fixed length static
 * hashmap) this is also a reasonable approach.
 * 
 * @author marcus
 *
 */
public class IndexContextThreadLocalImpl implements IndexCoordination {

	
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
	private Set<IndexMetadata<?,?>> metaDataSet;
		
	
	public IndexContextThreadLocalImpl() {
		metaDataSet = new HashSet<IndexMetadata<?,?>>();
	}
	
 	@Override
	public <C,K> void addIndexMetaData(IndexMetadata<C,K> metaData) {
		
 		metaDataSet.add(metaData);
		
	}
 	
	@Override
	public <C> boolean indexExists(IndexMappingKey<C> key) {
		return metaDataSet.contains(key);
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
		
		reading (new IndexMapping<C, V>(cf, columnName, null, value));
		
	}

	@Override
	public <C, V> void modifying(IndexMappingKey<C> key, V newValue)
			throws NoReadException {
		
		IndexMapping<C,V> mapping = (IndexMapping<C,V>)indexMapLocal.get();
		
		if (mapping == null)
			throw new NoReadException();
		
		mapping.setValueOfCol(newValue);
		
		
	}

	@Override
	public <C, V> void modifying(String cf, C columnName, V newValue)
			throws NoReadException {
		
		modifying(new IndexMappingKey(cf, columnName), newValue );
		
	}

	@Override
	public <C, V> IndexMapping<C, V> get(IndexMappingKey<C> key) {
		
		
		IndexMapping<C,V> indexMapping = (IndexMapping<C,V>)indexMapLocal.get().get(key);
		
		return indexMapping;
		
	}

	

}

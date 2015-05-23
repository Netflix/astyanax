package com.netflix.astyanax.cql.reads.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.cql.util.CqlTypeMapping;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

/**
 * Impl for {@link Rows} that parses the {@link ResultSet} from java driver and translates back to Astyanax Rows. 
 * Note that if your schema has a clustering key, then each individual row from the result set is a unique column, 
 * and all result set rows with the same partition key map to a unique Astyanax row. 
 * 
 * Note that this class leverages the cursor support from java driver and expects the user to use the iterator based 
 * approach when reading through results which contain multiple rows. 
 * 
 * Some users may want to read all the data instead of using an iterator approach. To handle this situation,
 * the class maintains some state that indicates how the object is first accessed in order to avoid iterating twice
 * over the same result set.
 * 
 * @author poberai
 *
 * @param <K>
 * @param <C>
 */
public class CqlRowListIterator<K,C> implements Rows<K,C> {

	private enum State {
		UnSet, PreFetch, PrefetchDone, Iterator;
	}
	
	private final ResultSet resultSet; 
	private final ColumnFamily<K,C> cf; 
	private final Serializer<K> keySerializer;
	private final boolean isClusteringKey;
	
	private final AtomicReference<State> stateRef = new AtomicReference<State>(State.UnSet);
	
	private final AtomicInteger iterRowCount = new AtomicInteger(0);
	private final AtomicReference<Iterator<Row<K,C>>> iterRef = new AtomicReference<Iterator<Row<K,C>>>(null);
	
	private final List<Row<K,C>>   rows = new ArrayList<Row<K,C>>();
	private final Map<K, Row<K,C>> lookup = new HashMap<K, Row<K,C>>();

	public CqlRowListIterator(ResultSet rs, ColumnFamily<K,C> cf) {
		this.resultSet = rs;
		this.cf = cf;
		this.keySerializer = cf.getKeySerializer();
		CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) cf.getColumnFamilyDefinition();
		this.isClusteringKey = cfDef.getClusteringKeyColumnDefinitionList().size() > 0;
	}
	
	@Override
	public Iterator<Row<K, C>> iterator() {
		
		if (iterRef.get() != null) {
			return iterRef.get();
			//throw new RuntimeException("Cannot re-iterate over rows while already iterating");
		}
		
		if (stateRef.get() == State.UnSet) {
			stateRef.set(State.Iterator);
		}
		
		Iterator<Row<K,C>> rowIter =  new Iterator<Row<K,C>>() {

			private final Iterator<com.datastax.driver.core.Row> rsIter = resultSet.iterator();
			private List<com.datastax.driver.core.Row> currentList = new ArrayList<com.datastax.driver.core.Row>();
			private K currentRowKey = null; 
			
			@Override
			public boolean hasNext() {
				if (!isClusteringKey) {
					return rsIter.hasNext();
				} else {
					return rsIter.hasNext() || !currentList.isEmpty();
				}
			}

			@Override
			public Row<K, C> next() {
				
//				if (!hasNext()) {
//					throw new IllegalStateException();
//				}

				if (isClusteringKey) {
					
					// Keep reading rows till we find a new rowKey, and then return the prefecthed list as a single row
					while (rsIter.hasNext()) {
						com.datastax.driver.core.Row rsRow = rsIter.next();
						K rowKey = (K) CqlTypeMapping.getDynamicColumn(rsRow, keySerializer, 0, cf);
						if (currentRowKey == null || rowKey.equals(currentRowKey)) {
							currentList.add(rsRow);
							currentRowKey = rowKey;
						} else {
							// Ok, we have read all columns of a single row. Return the current fully formed row
							List<com.datastax.driver.core.Row> newList = new ArrayList<com.datastax.driver.core.Row>();
							newList.addAll(currentList);
							
							// reset the currentList and start with the new rowkey
							currentList = new ArrayList<com.datastax.driver.core.Row>();
							currentList.add(rsRow);
							currentRowKey = rowKey;
							iterRowCount.incrementAndGet();

							return new CqlRowImpl<K,C>(newList, cf);
						}
					}
					
					// In case we got here, then we have exhausted the rsIter and can just return the last row
					List<com.datastax.driver.core.Row> newList = new ArrayList<com.datastax.driver.core.Row>();
					newList.addAll(currentList);
					
					// reset the currentList and start with the new rowkey
					currentList = new ArrayList<com.datastax.driver.core.Row>();
					iterRowCount.incrementAndGet();
					return new CqlRowImpl<K,C>(newList, cf);
					
				} else {
					// Here each cql row corresponds to a single Astyanax row
					if (rsIter.hasNext()) {
						com.datastax.driver.core.Row rsRow = rsIter.next();
						return new CqlRowImpl<K,C>(rsRow, cf);
					} else {
						return null; // this should not happen if this is all accessed via the iterator
					}
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
		
		iterRef.set(rowIter);
		return iterRef.get();
	}

	@Override
	public Collection<K> getKeys() {
		consumeAllRows();
		return lookup.keySet();
	}

	@Override
	public Row<K, C> getRow(K key) {
		consumeAllRows();
		return lookup.get(key);
	}

	@Override
	public Row<K, C> getRowByIndex(int i) {
		consumeAllRows();
		return rows.get(i);
	}

	@Override
	public int size() {
		if (stateRef.get() == State.Iterator) {
			return this.iterRowCount.get();
		} else {
			consumeAllRows();
			return rows.size();
		}
	}

	@Override
	public boolean isEmpty() {
		if (stateRef.get() == State.UnSet) {
			this.iterator(); // init the iterator
		}
		
		if (stateRef.get() == State.Iterator) {
			return !this.iterRef.get().hasNext();
		} else {
			consumeAllRows();
			return rows.size() == 0;
		}
	}
	
	private void consumeAllRows() {
		
		if (this.stateRef.get() == State.PrefetchDone) {
			return;
		}
		
		if (this.stateRef.get() == State.Iterator) {
			throw new RuntimeException("Cannot pre-fetch rows while iterating over rows");
		}
		
		this.stateRef.set(State.PreFetch);
		
		// Ok, we made it this far, we can now prefetch
		
		Iterator<Row<K,C>> rowIter = this.iterator();
		while (rowIter.hasNext()) {
			Row<K,C> row = rowIter.next();
			this.rows.add(row);
			this.lookup.put(row.getKey(), row);
		}
		
		this.iterRef.set(rows.iterator());
		stateRef.set(State.PrefetchDone);
	}

}

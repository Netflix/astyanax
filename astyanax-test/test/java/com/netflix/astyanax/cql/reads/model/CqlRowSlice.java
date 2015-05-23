package com.netflix.astyanax.cql.reads.model;

import java.util.Collection;

import com.netflix.astyanax.query.RowSliceQuery;

/**
 * Helper class that encapsulates a row slice for a {@link RowSliceQuery}
 * 
 * Note that there are 2 essential components for a row slice
 * 
 * 1. Collection of individual row keys
 * 2. Row range specification. 
 * 
 * The class has data structures to represent both these components and also has helpful methods to identify
 * the type of row slice query.
 * 
 * @author poberai
 *
 * @param <K>
 */
public class CqlRowSlice<K> {

	// Stuff needed for the direct query using the in() clause
	private Collection<K> keys; 
	private RowRange<K> range = new RowRange<K>();
	
	public static class RowRange<K> {
		// Stuff needed for the row range query
		private K startKey;
		private K endKey;
		private String startToken;
		private String endToken;
		int count; 
		
		public K getStartKey() {
			return startKey;
		}

		public K getEndKey() {
			return endKey;
		}

		public String getStartToken() {
			return startToken;
		}

		public String getEndToken() {
			return endToken;
		}

		public int getCount() {
			return count;
		}
	}
	
	public CqlRowSlice(Collection<K> keys) {
		this.keys = keys;
	}
	
	public CqlRowSlice(K startKey, K endKey, String startToken, String endToken, int count) {
		this.range.startKey = startKey;
		this.range.endKey = endKey;
		this.range.startToken = startToken;
		this.range.endToken = endToken;
		this.range.count = count;
	}
	
	public Collection<K> getKeys() {
		return keys;
	}
	
	public RowRange<K> getRange() {
		return range;
	}
	
	public boolean isCollectionQuery() {
		return this.keys != null && keys.size() > 0;
	}
	
	public boolean isRangeQuery() {
		return !isCollectionQuery() && range != null;
	}
}

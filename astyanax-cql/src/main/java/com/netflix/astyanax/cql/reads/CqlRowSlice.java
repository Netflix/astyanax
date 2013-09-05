package com.netflix.astyanax.cql.reads;

import java.util.Collection;

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

package com.netflix.astyanax.cql.reads;

import java.util.Collection;

public class CqlRowSlice<K> {

	// Stuff needed for the direct query using the in() clause
	private Collection<K> keys; 

	// Stuff needed for the row range query
	private K startKey;
	private K endKey;
	private String startToken;
	private String endToken;
	int count; 
	
	public CqlRowSlice(Collection<K> keys) {
		this.keys = keys;
	}
	
	public CqlRowSlice(K startKey, K endKey, String startToken, String endToken, int count) {
		this.startKey = startKey;
		this.endKey = endKey;
		this.startToken = startToken;
		this.endToken = endToken;
		this.count = count;
	}
	
	public Collection<K> getKeys() {
		return keys;
	}
	
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

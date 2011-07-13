package com.netflix.astyanax.model;

import java.util.Collection;


/**
 * Definition of a set of keys.  The set can be either a fixed set of keys,
 * a range of keys or a range of tokens.
 * @author elandau
 *
 * @param <K>
 */
public class KeySlice<K> {
	private Collection<K> keys;
	private K startKey;
	private K endKey;
	private String startToken;
	private String endToken;
	private int size = 0;
	
	public KeySlice(Collection<K> keys) {
		this.keys = keys;
	}
	
	public KeySlice(K startKey, K endKey, String startToken, String endToken, int size) {
		this.startKey = startKey;
		this.endKey = endKey;
		this.startToken = startToken;
		this.endToken = endToken;
		this.size = size;
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
	
	public int getLimit() { 
		return size;
	}
}

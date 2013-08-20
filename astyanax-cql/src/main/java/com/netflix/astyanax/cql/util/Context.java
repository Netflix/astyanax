package com.netflix.astyanax.cql.util;

import com.datastax.driver.core.Cluster;
import com.netflix.astyanax.model.ColumnFamily;

public class Context<V> {

	private V value; 
	
	public Context(V value) {
		this.value = value; 
	}
	
	public V get() {
		return value;
	}

	public static class ColumnFamilyCtx {

		public Cluster cluster;
		public String keyspace;
		public ColumnFamily<?, ?> columnFamily; 
		
		public ColumnFamilyCtx(Cluster c, String ks, ColumnFamily<?,?> cf) {
			cluster = c;  keyspace = ks; columnFamily = cf; 
		}
	}
}



package com.netflix.astyanax.index;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.BytesArraySerializer;

public class SchemaIndexUtil {

	public static String CACHE_KEY = "caching";
	public static String CACHE_VAL = "ALL";
	
	public static void createIndexCF(Keyspace keyspace,String CFName,boolean drop,boolean cacheAll) throws ConnectionException  {
		
		ColumnFamily<byte[], byte[]> index_cf = ColumnFamily.newColumnFamily(
				CFName, BytesArraySerializer.get(),
				BytesArraySerializer.get());
		
		//DROP
		if (drop) {
			keyspace.dropColumnFamily(index_cf);
		
		}
		//CREATE
		if (cacheAll)
			keyspace.createColumnFamily(index_cf, ImmutableMap.<String, Object> builder().put("caching", "ALL").build());
		else
			keyspace.createColumnFamily(index_cf,new HashMap<String,Object>());
		

	}
	
	public static void createIndexCF(Keyspace keyspace,String CFName,boolean drop,Map<String,Object> options) throws ConnectionException {
		
		ColumnFamily<byte[], byte[]> index_cf = ColumnFamily.newColumnFamily(
				CFName, BytesArraySerializer.get(),
				BytesArraySerializer.get());
		
		//DROP
		if (drop) {
			keyspace.dropColumnFamily(index_cf);
		
		}
		keyspace.createColumnFamily(index_cf,options);
	}
	
}

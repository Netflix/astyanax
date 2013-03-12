package com.netflix.astyanax.index;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.BytesArraySerializer;

public class SchemaIndexUtil {

	public static String CACHE_KEY = "caching";
	public static String CACHE_VAL = "ALL";
	
	private static Logger LOG = LoggerFactory.getLogger(SchemaIndexUtil.class);
	 
	public static void createIndexCF(Keyspace keyspace,String CFName,boolean drop,boolean cacheAll) throws ConnectionException  {
		
		ColumnFamily<byte[], byte[]> index_cf = ColumnFamily.newColumnFamily(
				CFName, BytesArraySerializer.get(),
				BytesArraySerializer.get());
		
		//DROP
		if (drop) {
			try {
				keyspace.dropColumnFamily(index_cf);
			}catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
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

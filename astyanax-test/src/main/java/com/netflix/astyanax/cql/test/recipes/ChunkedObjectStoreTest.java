package com.netflix.astyanax.cql.test.recipes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.cql.test.KeyspaceTests;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.recipes.storage.CassandraChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ObjectMetadata;
import com.netflix.astyanax.serializers.StringSerializer;

public class ChunkedObjectStoreTest extends KeyspaceTests {

	public static ColumnFamily<String, String> CF_CHUNK = ColumnFamily.newColumnFamily("cfchunk", StringSerializer.get(), StringSerializer.get());

	@BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.createColumnFamily(CF_CHUNK, null);
		CF_CHUNK.describe(keyspace);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(CF_CHUNK);
	}
	
	@Test
	public void testAll() throws Exception {
		
		CassandraChunkedStorageProvider provider = new CassandraChunkedStorageProvider(keyspace, CF_CHUNK);
		
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<100; i++) {
			sb.append("abcdefghijklmnopqrstuvwxyz_");
		}
		
		String input = sb.toString();
		
		ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes());
		
		ObjectMetadata meta = ChunkedStorage.newWriter(provider, "MyObject", in)
			    .withChunkSize(100)
			    .call();
		
		meta = ChunkedStorage.newInfoReader(provider, "MyObject").call();
		System.out.println(meta.getObjectSize().intValue());
		System.out.println(meta.getChunkCount());
		
		ByteArrayOutputStream os = new ByteArrayOutputStream(meta.getObjectSize().intValue());
		
		meta = ChunkedStorage.newReader(provider, "MyObject", os)
				  .withBatchSize(11)       // Randomize fetching blocks within a batch. 
				  .withConcurrencyLevel(3)
				.call();
		
		String output = os.toString();
		
		Assert.assertEquals(input, output);
		
		ChunkedStorage.newDeleter(provider, "MyObject").call();
		
		for (int i=0; i<meta.getChunkCount(); i++) {
			ColumnList<String> result = keyspace.prepareQuery(CF_CHUNK).getKey("MyObject$" + i).execute().getResult();
			Assert.assertTrue(result.isEmpty());
		}
	}
}

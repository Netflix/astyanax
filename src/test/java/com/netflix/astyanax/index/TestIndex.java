package com.netflix.astyanax.index;

import java.util.Collection;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class TestIndex {

	static AstyanaxContext<Keyspace> context;
	static Keyspace keyspace;
	
	public static void init() {
		context = new AstyanaxContext.Builder()
				.forCluster("ClusterName")
				.forKeyspace("icrskeyspace")
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setDiscoveryType(NodeDiscoveryType.NONE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setPort(9160).setMaxConnsPerHost(1)
								.setSeeds("127.0.0.1:9160"))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		
		keyspace = context.getEntity();

	}
	@BeforeClass
	public static void beforeClass() {
		init();
	}
	@Test
	public void testPutStringIndex() throws Exception {
		
		
		MutationBatch m = keyspace.prepareMutationBatch();
		Index<String,String,String> ind = new IndexImpl<String, String, String>( keyspace, m,"index_cf" );
		
		String colToBeIndexed = "blaa";
		
		ind.insertIndex(colToBeIndexed, "tobeindexed", "pk value");
		ind.insertIndex(colToBeIndexed, "tobeindexed2", "pk value 2");
		
		m.execute();
		
		Collection<String> indexResult = ind.eq("blaa", "tobeindexed");
		Assert.assertEquals(1, indexResult.size());
		
		
		
	}
	
	@Test
	public void multiPK() throws Exception {
		
		
		MutationBatch m = keyspace.prepareMutationBatch();
		Index<String,String,String> ind = new IndexImpl<String, String, String>( keyspace, m,"index_cf" );
		
		String colToBeIndexed = "multiCol";
		String indexedVal = "pin123456";
		
		ind.insertIndex(colToBeIndexed, indexedVal, "pk value");
		ind.insertIndex(colToBeIndexed, indexedVal, "pk value 2");
		
		m.execute();
		
		Collection<String> indexResult = ind.eq(colToBeIndexed, indexedVal);
		Assert.assertEquals(2, indexResult.size());
		
		
		
	}
	
	
	@Test
	public void testPutLongPKIndex() throws Exception {
		
		
		MutationBatch m = keyspace.prepareMutationBatch();
		Index<String,String,Long> ind = new IndexImpl<String, String, Long>( keyspace,m,"test_cf" );
		
		String colToBeIndexed = "blaa";
		Long pkVal = new Long(1234567);
		ind.insertIndex(colToBeIndexed, "tobeindexed", pkVal);
		
		
		m.execute();
		
		Collection<Long> indexResult = ind.eq(colToBeIndexed, "tobeindexed");
		
		Assert.assertEquals(1, indexResult.size());
		
		Assert.assertEquals(indexResult.iterator().next(), pkVal);
		
		
		
	}
	@Test
	public void testPutIntegerPKIndex() throws Exception {
		
		
		MutationBatch m = keyspace.prepareMutationBatch();
		Index<String,String,Integer> ind = new IndexImpl<String, String, Integer>( keyspace,m,"test_cf" );
		
		String colToBeIndexed = "intcol";
		int pkVal = 1234567;
		ind.insertIndex(colToBeIndexed, "tobeindexed", pkVal);
		
		
		m.execute();
		
		Collection<Integer> indexResult = ind.eq(colToBeIndexed, "tobeindexed");
		
		Assert.assertEquals(1, indexResult.size());
		Assert.assertEquals(indexResult.iterator().next(), (Integer)pkVal);
		
		
	}
	@Test
	public void testBytePKIndex() throws Exception {
		
		
		//TODO
		
		
	}
	
	@Test
	public void testObjectPKIndex() throws Exception {
		
		
		//TODO
		
		
	}
	
	@Test
	public void buildIndex() throws Exception {
		
		MutationBatch m = keyspace.prepareMutationBatch();
		Index<String,String,String> ind = new IndexImpl<String, String, String>( keyspace, m, "test_cf" );
		
	
		ind.buildIndex("device_service", "srv_id", String.class);
		
		m.execute();
		
	}

}

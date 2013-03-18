package com.netflix.astyanax.index;

import java.util.Collection;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;

public class PlainIndexTest {

	static AstyanaxContext<Keyspace> context;
	static Keyspace keyspace;
	
	//static EmbeddedCassandra cassandra;
	
	@BeforeClass
	public static void beforeClass() throws Exception {
		
		SingletonEmbeddedCassandra.getInstance();
		context = SetupUtil.initKeySpace();
		keyspace = context.getEntity();
		
		SetupUtil.indexCFSetup(keyspace);
		SetupUtil.devSrvDataSetup(keyspace);
		
	}
	@AfterClass
    public static void teardown() {
        if (context != null)
            context.shutdown();
        
       //SetupUtil.stopCassandra();
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

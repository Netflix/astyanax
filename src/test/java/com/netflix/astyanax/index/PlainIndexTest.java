package com.netflix.astyanax.index;

import java.io.Serializable;
import java.util.Collection;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Composite;
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
	@Ignore
	public void testObjectPKIndex() throws Exception {
		
		class PsuedoCompositeKey implements Serializable {
			String key;
			PsuedoCompositeKey(String key) {
				this.key = key;
			}
		}
		
		class PsuedoCompositeValue implements Serializable {
			String name;
			String value;
			String desc;
			PsuedoCompositeValue(String name,String value,String desc) {
				this.name = name;this.value = value; this.desc = desc;
			}
		}
		
		MutationBatch m = keyspace.prepareMutationBatch();
		Index<String,PsuedoCompositeValue,PsuedoCompositeKey> ind = new IndexImpl<String, PsuedoCompositeValue, PsuedoCompositeKey>( keyspace,m,"test_obj_cf" );
		
		String colToBeIndexed = "intcol";
		
		ind.insertIndex( colToBeIndexed,new PsuedoCompositeValue("name","val","des"),new PsuedoCompositeKey("a key"));
			
		
		
	}
	//this doesn't work
	@Test
	public void testAllComposites() throws Exception {
		
		MutationBatch m = keyspace.prepareMutationBatch();
		Index<Composite,Composite,String> ind = new IndexImpl<Composite, Composite, String>(keyspace,m,"index_cf_composite");
		Composite colVal = new Composite("Happy",new Long(0));
		Composite col = new Composite("Family");
		Composite pkVal = new Composite("Makes","the","world","goround");
		ind.insertIndex(col,colVal , "not composite");
		
		
		Collection<String> indexResult = ind.eq(col, colVal);
		
		Assert.assertEquals(1, indexResult.size());
		
		
		
	}
	
	//this one works
	@Test
	public void testCompositeNonCompositePKValue() throws Exception {
		
		MutationBatch m = keyspace.prepareMutationBatch();
		Index<Composite,Composite,String> ind = new IndexImpl<Composite, Composite, String>(keyspace,m,"index_cf_composite");
		Composite colVal = new Composite("Happy",new Long(0));
		Composite col = new Composite("Family");
		Composite pkVal = new Composite("Makes","the","world","goround");
		ind.insertIndex(col,colVal , "not composite");
		
		
		Collection<String> indexResult = ind.eq(col, colVal);
		
		Assert.assertEquals(1, indexResult.size());
		
		
		
	}
	
	@Test
	public void buildIndex() throws Exception {
		
		MutationBatch m = keyspace.prepareMutationBatch();
		Index<String,String,String> ind = new IndexImpl<String, String, String>( keyspace, m, "test_cf" );
		
	
		ind.buildIndex("device_service", "srv_id", String.class);
		
		m.execute();
		
	}

}

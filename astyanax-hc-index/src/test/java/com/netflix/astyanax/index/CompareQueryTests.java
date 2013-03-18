package com.netflix.astyanax.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.model.ThriftRowsSliceImpl;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;


public class CompareQueryTests {

	
	static Keyspace keyspace;
	static Cluster cluster;
	static AstyanaxContext<Cluster> context;
	static ColumnFamily<String, String> secIndCF;
	static ColumnFamily<String, String> hcIndCF;	
		
	static IndexCoordination indexCoordination;
	
	@BeforeClass
	public static void setup() throws Exception {
		
		SingletonEmbeddedCassandra.getInstance();
		//this seems primitive??
		Thread.sleep(SetupUtil.SERVER_START_TIME);
	
		context = SetupUtil.initCluster();
		cluster = context.getEntity();
		keyspace = cluster.getKeyspace(context.getKeyspaceName());
						
		SetupUtil.indexCFSetup(keyspace);
		hcIndCF = SetupUtil.hcIndexDataSetup(keyspace);
		secIndCF = (ColumnFamily<String, String>) SetupUtil.secondaryIndexDataSetup(keyspace, cluster);
		
		indexCoordination = SetupUtil.initIndexCoordinator(hcIndCF.getName(), "col1");
	}
	
	@AfterClass
    public static void teardown() {
        if (context != null)
            context.shutdown();
        
       //SetupUtil.stopCassandra();
    }
			
	
	@Test
	public void queryHCIndexDataTests() throws Exception {		
		
		String indexedValue = "ind1_100998880";
		
		MutationBatch batch = keyspace.prepareMutationBatch();
		Index<String, String, String> ind = new IndexImpl<String, String, String>(
				keyspace, batch, hcIndCF.getName());
		ind.buildIndex(hcIndCF.getName(), "col1", String.class);
		batch.execute();

		Collection<String> keys = ind.eq("col1", indexedValue);
		Assert.assertEquals(2, keys.size());

		HCIndexQueryImpl<String, String, String> hcq = new HCIndexQueryImpl<String, String, String>(keyspace, hcIndCF);
		
		validateRowsResult(hcq.equals("col1", indexedValue).execute().getResult(), 
				indexedValue.concat(":ind2_sv_1"), indexedValue.concat(":ind2_sv_2"));
		
		IndexMapping<String, String> mapping = indexCoordination
				.get(new IndexMappingKey<String>(hcIndCF.getName(), "col1"));

		Assert.assertEquals(indexedValue, mapping.getOldValueofCol());

		
	}
		
	@Test
	public void querySecondaryIndexDataTests() throws Exception {		
		String indexedValue = "ind1_100998880";
		
		ByteBufferRange range = new RangeBuilder().setStart("0", StringSerializer.get()).setEnd("~", StringSerializer.get()).setLimit(1000).setReversed(false).build();
		Rows<String, String> rowResults = keyspace.prepareQuery(secIndCF).searchWithIndex().addExpression().whereColumn("col1").equals().value(indexedValue).withColumnRange(range).execute().getResult();
		
		validateRowsResult(rowResults, 
				indexedValue.concat(":ind2_sv_1"), indexedValue.concat(":ind2_sv_2"));
	}
	
	@Test
	public void queryMultipleKeysDataTests() throws Exception {		
		String indexedValue = "ind1_100998880";
		String[] keys = new String[]{indexedValue.concat(":ind2_sv_1"), indexedValue.concat(":ind2_sv_2")};
		
		ByteBufferRange range = new RangeBuilder().setStart("0", StringSerializer.get()).setEnd("~", StringSerializer.get()).setLimit(1000).setReversed(false).build();				
		Rows<String, String> rowResults = keyspace.prepareQuery(secIndCF).getRowSlice(keys).withColumnRange(range).execute().getResult();
		
		validateRowsResult(rowResults, 
				indexedValue.concat(":ind2_sv_1"), indexedValue.concat(":ind2_sv_2"));		
	}
	
	public void validateRowsResult(Rows<String, String> rowResults, String... expectedKeys){
		if(rowResults instanceof ThriftRowsSliceImpl){
			System.out.println("instance of ThriftRowsListImpl");
		}
		Assert.assertEquals(2, expectedKeys.length);
		
		List<String> expectedKeyList = new LinkedList<String>(Arrays.asList(expectedKeys));		
		Iterator<Row<String, String>> iter = rowResults.iterator();		
			
		while(iter.hasNext()){			
			Row<String, String> row = iter.next();
			Assert.assertTrue(expectedKeyList.contains(row.getKey()));

			//If ThriftRowsListImpl is the implementation class for Rows then rawKey is empty otherwise it matches the key when serialized.
			//TODO This might be a bug.
			String expectedRawKey = (rowResults instanceof ThriftRowsSliceImpl)?row.getKey():"";
			Assert.assertEquals(expectedRawKey, StringSerializer.get().fromByteBuffer(row.getRawKey()));

			expectedKeyList.remove(row.getKey());					
		}				
		Assert.assertTrue(expectedKeyList.isEmpty());
	}
	
}

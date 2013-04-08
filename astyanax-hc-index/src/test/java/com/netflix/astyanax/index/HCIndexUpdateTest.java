package com.netflix.astyanax.index;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;

/*
 * 
 
 
 
 */
public class HCIndexUpdateTest {

	static AstyanaxContext<Keyspace> context;
	
	
	static IndexCoordination indexCoordination;
	static Keyspace keyspace;
	
	HighCardinalityQuery<String, String, String> hcq = null;
	MutationBatch indBatchMutator = null;
	ColumnFamily<String, String> CF;
	
	@BeforeClass
	public static void setup() throws Exception {
		
		SingletonEmbeddedCassandra.getInstance();
		//this seems primitive??
		Thread.sleep(SetupUtil.SERVER_START_TIME);

		context = SetupUtil.initKeySpace();
		keyspace = context.getEntity();
		indexCoordination = SetupUtil.initIndexCoordinator();
		SetupUtil.indexCFSetup(keyspace);
		SetupUtil.devSrvDataSetup(keyspace);
		
	}
	@AfterClass
    public static void teardown() {
        if (context != null)
            context.shutdown();
        
       //SetupUtil.stopCassandra();
    }
		
	
	@Before
	public void before() {
		CF = new ColumnFamily<String, String>("device_service", StringSerializer.get(), StringSerializer.get());
		
		hcq = new HCIndexQueryImpl<String, String, String>(keyspace, CF);
		indBatchMutator = new HCMutationBatchImpl(keyspace.prepareMutationBatch());
		
	}
	
	
	@Test
	public void queryIndexDataAndUpdate() throws Exception {
		MutationBatch batch = keyspace.prepareMutationBatch();
		Index<String,String,String> ind = new IndexImpl<String, String, String>( keyspace, batch,"device_service" );
		ind.buildIndex("device_service", "pin", String.class);
		batch.execute();
				
		
		RowSliceQuery<String, String> sliceQuery = hcq.equals("pin", "100998880");
		
		Rows<String,String> rowResults= sliceQuery.execute().getResult();
		Row<String,String> row = rowResults.getRowByIndex(0);
		
		//ensure that the index coordinator is reading:
		IndexCoordination coordinator = IndexCoordinationFactory.getIndexContext();
		IndexMapping<String,String> mapping = coordinator.get(new IndexMappingKey<String>("device_service", "pin"));
		Assert.assertEquals("100998880", mapping.getValueOfCol() );
		Assert.assertEquals("100998880", mapping.getOldValueofCol() );
		
		//Now update the index
		batch = keyspace.prepareMutationBatch();
		MutationBatch indexedBatch = new HCMutationBatchImpl(batch);
		ColumnListMutation<String> mutation = indexedBatch.withRow(CF, row.getKey());
		
		mutation.putColumn("pin", "100998880_m");
		mapping = coordinator.get(new IndexMappingKey<String>("device_service", "pin") );
		Assert.assertEquals("100998880_m", mapping.getValueOfCol() );
		Assert.assertEquals("100998880", mapping.getOldValueofCol() );
		batch.execute();
		
		//Now read from it.
		
		rowResults = hcq.equals("pin", "100998880_m").execute().getResult();
		
		Assert.assertEquals(1,rowResults.size());
		
		
	}
	
	@Test
	public void getupdatedvalue() throws Exception {
		
		MutationBatch batch = keyspace.prepareMutationBatch();
		Index<String,String,String> ind = new IndexImpl<String, String, String>( keyspace, batch,"device_service" );
		ind.insertIndex( "pin", "100998880","100998880:srv_1");
		ind.updateIndex("pin","100998880_m", "100998880", "100998880:srv_1");
		batch.execute();
		
		RowSliceQuery<String, String> sliceQuery = hcq.equals("pin", "100998880_m");
		Rows<String,String> results = sliceQuery.execute().getResult();
		
		
		
	}
	
	
	@Test
	public void testRepair() throws Exception {
		
		MutationBatch batch = keyspace.prepareMutationBatch();
		Index<String,String,String> ind = new IndexImpl<String, String, String>( keyspace, batch,"device_service" );
		ind.buildIndex("device_service", "pin", String.class);
		batch.execute();
				
		
		RowSliceQuery<String, String> sliceQuery = hcq.equals("pin", "100998880");
		
		Rows<String,String> rowResults= sliceQuery.execute().getResult();
		Row<String,String> row = rowResults.getRowByIndex(0);
		//Row<String,String> row2 = rowResults.getRowByIndex(1);
		
		//ensure that the index coordinator is reading:
		IndexCoordination coordinator = IndexCoordinationFactory.getIndexContext();
		IndexMapping<String,String> mapping = coordinator.get(new IndexMappingKey<String>("device_service", "pin"));
		Assert.assertEquals("100998880", mapping.getValueOfCol() );
		Assert.assertEquals("100998880", mapping.getOldValueofCol() );
		
		//Now update the index - without going through HC update interface
		batch = keyspace.prepareMutationBatch();
		ColumnListMutation<String> mutation = batch.withRow(CF, row.getKey());
		mutation.putColumn("pin", "100998880_m");
		batch.execute();
		
		//then we insert to the index (there are a couple of ways we can 
		//arrive here)
		batch = keyspace.prepareMutationBatch();
		ind = new IndexImpl<String, String, String>( keyspace, batch,"device_service" );
		//only modify the first row - the second will still be a false positive.
		ind.insertIndex("pin", "100998880_m",row.getKey());
		batch.execute();
		
		//Now read from it - with a read listener attached.
		//from the old value that didn't get replaced
		CountDownLatch latch = new CountDownLatch(1);
		hcq.registerRepairListener(new TestRepairListener(latch));
		rowResults = hcq.equals("pin", "100998880").execute().getResult();
		
		Assert.assertTrue( latch.await(1, TimeUnit.SECONDS) );
		
		//Assert.assertEquals(1,rowResults.size());
	}
	
	
	

}

class TestRepairListener implements RepairListener<String, String, String> {

	CountDownLatch latch = null;
	
	public TestRepairListener(CountDownLatch latch) {
		this.latch = latch;
	}
	@Override
	public void onRepair(IndexMapping<String, String> mapping, String key) {
		System.out.println("Repaired key: " + key + "value: " + mapping.getValueOfCol() + " old_value: " + mapping.getOldValueofCol());
		latch.countDown();
		
	}
	
}

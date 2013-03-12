package com.netflix.astyanax.index;

import java.util.Collection;
import java.util.Iterator;

import junit.framework.Assert;

import org.codehaus.jackson.map.ser.std.StdArraySerializers.ByteArraySerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.test.EmbeddedCassandra;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;

/*
 * 
 
 
 
 */
public class HCIndexCustomIndexCFTest {

	static AstyanaxContext<Keyspace> context;
	
	
	static IndexCoordination indexCoordination;
	static Keyspace keyspace;
	
	HighCardinalityQuery<String, String, String> hcq = null;
	IndexedMutationBatch indBatchMutator = null;
	ColumnFamily<String, String> CF;
	
	@BeforeClass
	public static void setup() throws Exception {
		
		SingletonEmbeddedCassandra.getInstance();
		//this seems primitive??
		Thread.sleep(SetupUtil.SERVER_START_TIME);
		context = SetupUtil.initKeySpace();
		keyspace = context.getEntity();
		
		//sets up device_service data
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
		indBatchMutator = new HCMutationBatchImpl();
		
	}
	
	
	@Test
	public void queryIndexDataAndUpdate_customIndex() throws Exception {
		
		String customIndexCF = "index2";
		//prepare the index coordinator + the index CF
		IndexCoordination coordinator = IndexCoordinationFactory.getIndexContext();
		coordinator.addIndexMetaDataAndSchema(keyspace, new IndexMetadata<String, String>("device_service", "pin", String.class, customIndexCF));
		
		MutationBatch batch = keyspace.prepareMutationBatch();
		Index<String,String,String> ind = new IndexImpl<String, String, String>( keyspace, batch,"device_service",customIndexCF );
		ind.buildIndex("device_service", "pin", String.class);
		batch.execute();
				
		
		RowSliceQuery<String, String> sliceQuery = hcq.equals("pin", "100998880");
		
		Rows<String,String> rowResults= sliceQuery.execute().getResult();
		Row<String,String> row = rowResults.getRowByIndex(0);
				
		//Now update the index
		batch = keyspace.prepareMutationBatch();
		IndexedMutationBatch indexedBatch = new HCMutationBatchImpl();
		ColumnListMutation<String> mutation = indexedBatch.withIndexedRow(batch, CF, row.getKey());
		
		mutation.putColumn("pin", "100998880_m");
		
		batch.execute();
		
		//Now read from it.
		
		rowResults = hcq.equals("pin", "100998880_m").execute().getResult();
		
		Assert.assertEquals(1,rowResults.size());
		
		//ensure it was written to the correct CF (the custom one):
		ColumnFamily<byte[], byte[]> indexCF = new ColumnFamily<byte[], byte[]>(customIndexCF, BytesArraySerializer.get(), BytesArraySerializer.get());
		Rows<byte[],byte[]> allRows = keyspace.prepareQuery(indexCF).getAllRows().execute().getResult();
		
		Iterator<Row<byte[],byte[]>> iter = allRows.iterator();
		
		while (iter.hasNext()) {
			
			System.out.println("key: " + iter.next().getKey());
			
		}
		
		
		
	}
	
	//@Test
	public void getupdatedvalue() throws Exception {
		
		MutationBatch batch = keyspace.prepareMutationBatch();
		Index<String,String,String> ind = new IndexImpl<String, String, String>( keyspace, batch,"device_service" );
		ind.insertIndex( "pin", "100998880","100998880:srv_1");
		ind.updateIndex("pin","100998880_m", "100998880", "100998880:srv_1");
		batch.execute();
		
		RowSliceQuery<String, String> sliceQuery = hcq.equals("pin", "100998880_m");
		Rows<String,String> results = sliceQuery.execute().getResult();
		
		
		
	}
	
	
	

}

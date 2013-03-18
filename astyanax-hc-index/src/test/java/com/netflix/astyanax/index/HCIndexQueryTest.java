package com.netflix.astyanax.index;

import java.util.Collection;
import java.util.Date;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.test.EmbeddedCassandra;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;


public class HCIndexQueryTest {

	
	static Keyspace keyspace;
	static AstyanaxContext<Keyspace> context;
	
	private static EmbeddedCassandra cassandra;
	
	static IndexCoordination indexCoordination;
	HighCardinalityQuery<String, String, String> hcq = null;

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
		ColumnFamily<String, String> CF = new ColumnFamily<String, String>(
				"device_service", StringSerializer.get(),
				StringSerializer.get());

		hcq = new HCIndexQueryImpl<String, String, String>(keyspace, CF);
	}

	@Test
	public void testEmpty() throws Exception {

		RowSliceQuery<String, String> sliceQuery = hcq.equals("", "");

		sliceQuery.withColumnSlice("srv_id", "srv_type", "pin");

		Rows<String, String> rowResults = sliceQuery.execute().getResult();
		Assert.assertTrue(rowResults.isEmpty());

	}

	@Test
	public void testNoData() throws Exception {

		RowSliceQuery<String, String> sliceQuery = hcq.equals("one", "two")
				.withColumnSlice("srv_id", "srv_type");

		Rows<String, String> rowResults = sliceQuery.execute().getResult();
		Assert.assertTrue(rowResults.isEmpty());

	}

	@Test
	public void queryIndexData() throws Exception {
		MutationBatch batch = keyspace.prepareMutationBatch();
		Index<String, String, String> ind = new IndexImpl<String, String, String>(
				keyspace, batch, "device_service");
		ind.buildIndex("device_service", "pin", String.class);
		batch.execute();

		Collection<String> keys = ind.eq("pin", "100998880");
		Assert.assertEquals(2, keys.size());

		RowSliceQuery<String, String> sliceQuery = hcq.equals("pin",
				"100998880");

		Rows<String, String> rowResults = sliceQuery.execute().getResult();
		Assert.assertEquals(2, rowResults.size());

		IndexMapping<String, String> mapping = indexCoordination
				.get(new IndexMappingKey<String>("device_service", "pin"));

		Assert.assertEquals("100998880", mapping.getOldValueofCol());

	}

}

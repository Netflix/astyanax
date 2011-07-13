package com.netflix.astyanax.mock;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.KsDef;
import org.apache.log4j.Logger;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.MillisecondsClock;
import com.netflix.astyanax.mock.MockConstants;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftKeyspaceImpl;
import com.netflix.cassandra.EmbeddedCassandraInstance;
import com.netflix.cassandra.config.ConnectionPoolType;
import com.netflix.cassandra.config.LoadBalancingStrategyType;
import com.netflix.cassandra.config.NodeDiscoveryType;

public class MockEmbeddedCassandra {
    static final Logger LOG = Logger.getLogger(MockEmbeddedCassandra.class);

    private EmbeddedCassandraInstance cassandra;
    
    public void setup() throws Exception   {
		LOG.info("Starting embedded cassandra");
		cassandra = new EmbeddedCassandraInstance(
				new File("build/test/cassandra").getCanonicalFile(),
				MockConstants.CLUSTER_NAME,
				MockConstants.PORT,
				MockConstants.DATA_PORT);
		cassandra.start();
		
		Thread.sleep(MockConstants.DAEMON_START_WAIT);
		
        ColumnDef cd = new ColumnDef()
        	.setName(StringSerializer.get().toBytes("Index1"))
        	.setIndex_type(IndexType.KEYS)
        	.setValidation_class("BytesType");
        
        CfDef cf = new CfDef(MockConstants.KEYSPACE_NAME, MockConstants.CF_STANDARD1.getName())
        	.setColumn_type("Standard")
        	.setColumn_metadata(Arrays.asList(cd));

        CfDef scf = new CfDef(MockConstants.KEYSPACE_NAME, MockConstants.CF_SUPER1.getName())
        	.setColumn_type("Super");
        
        CfDef ccf = new CfDef(MockConstants.KEYSPACE_NAME, MockConstants.CF_COUNTER1.getName())
        	.setColumn_type("Standard")
        	.setDefault_validation_class("CounterColumnType");
        
        CfDef sccf = new CfDef(MockConstants.KEYSPACE_NAME, MockConstants.CF_COUNTER_SUPER1.getName())
        	.setColumn_type("Super")
        	.setDefault_validation_class("CounterColumnType");
        
        CfDef compositeCf = new CfDef(MockConstants.KEYSPACE_NAME, MockConstants.CF_COMPOSITE.getName())
    		.setColumn_type("Standard");
//    		.setComparator_type("compositecomparer.CompositeType");
        
        Map<String,String> stratOptions = new HashMap<String,String>();
        stratOptions.put("replication_factor", "1");
        
        KsDef ks = new KsDef()
        	.setStrategy_class(SimpleStrategy.class.getName())
        	.setStrategy_options(stratOptions)
        	.setName(MockConstants.KEYSPACE_NAME)
        	.setCf_defs(Arrays.asList(cf, scf, ccf, sccf, compositeCf));
        
        cassandra.createKeyspace(ks);
        
        populateTestData();
    }
    
    public void teardown()   {
    	if (cassandra != null) {
    		cassandra.stop();
    	}
    }
    
    private static void populateTestData() throws Exception{
    	LOG.info("Starting populateTestData...");
    	
    	ConnectionPoolConfigurationImpl config 
			= new ConnectionPoolConfigurationImpl(MockConstants.CLUSTER_NAME, MockConstants.KEYSPACE_NAME);
	
		config.setSeeds("127.0.0.1:"+MockConstants.PORT);
		config.setPort(MockConstants.PORT);
		config.setSocketTimeout(30000);
		config.setMaxTimeoutWhenExhausted(30);
		config.setClock(new MillisecondsClock());
		config.setLoadBlancingStrategyFactory(LoadBalancingStrategyType.ROUND_ROBIN);
		config.setNodeDiscoveryFactory(NodeDiscoveryType.RING_DESCRIBE);
		config.setConnectionPoolFactory(ConnectionPoolType.HOST_PARTITION);
		
		LOG.info(config);
		Keyspace keyspace = new ThriftKeyspaceImpl(config);
		keyspace.start();
		
		try {
			//
			// CF_Super : 
			//		'A' :
			//			'a' :
			//				1 : 'Aa1',
			//				2 : 'Aa2',
			//			'b' :
			//				...
	    	//			'z' :
	    	//				...
			//		'B' :
			//			...
			//
			// CF_Standard : 
			//		'A' :
			//			'a' : 1,
			//			'b' : 2,
			//				...
	    	//			'z' : 26,
			//		'B' :
			//			...
			//
	
			MutationBatch m;
			OperationResult<Void> result;
			m = keyspace.prepareMutationBatch();
			
			for (char keyName ='A'; keyName <= 'Z'; keyName++) {
				String rowKey = Character.toString(keyName);
				ColumnListMutation<String> cfmSuper = m.withRow(MockConstants.CF_SUPER1, rowKey);
				ColumnListMutation<String> cfmStandard = m.withRow(MockConstants.CF_STANDARD1, rowKey);
				for (char scName = 'a'; scName <= 'z'; scName++) {
					cfmStandard.putColumn(Character.toString(scName), (int)(scName - 'a')+1, null);
					cfmStandard.putColumn("Index1", (int)(scName - 'a')+1, null);
	
					/*
					// Super Column
					ColumnListMutation<Integer> clm = cfmSuper.withSuperColumn(
						new ColumnPath<Integer>(IntegerSerializer.get())
							.append(Character.toString(scName), StringSerializer.get()));
					
					for (int i = 1; i <= 10; i++) {
						clm.putColumn(i, rowKey+scName+i, null);
					}
					*/
				}
			}
			
			m.withRow(MockConstants.CF_STANDARD1, "Prefixes")
				.putColumn("Prefix1_a", 1, null)
				.putColumn("Prefix1_b", 2, null)
				.putColumn("prefix2_a", 3, null);
	
			result = m.execute();
			System.out.printf("executed on %s in %d msec\n", 
					result.getHost(),
					result.getLatency());
		}
		finally {
			keyspace.shutdown();
		}
    	LOG.info("... populateTestData done");
	}
    
    
    /*
    public static void initLibraries() {
    	
        Properties props = new Properties();
        
        props.setProperty("platform.ListOfComponentsToInit", listOfComponentsToInit);
     
        System.setProperty("netflix.environment", "test");
        System.setProperty("netflix.logging.realtimetracers", "true");
        System.setProperty("netflix.appinfo.name", "nfcassandra.unittest");
        
        String prefix = CLUSTER_NAME + "." + KEYSPACE_NAME + ".nfcassandra.";
        
        props.setProperty(prefix + "readConsistency", "CL_QUORUM");
        props.setProperty(prefix + "servers", "127.0.0.1:"+PORT);
        props.setProperty(prefix + "port", Integer.toString(PORT));
        props.setProperty(prefix + "socketTimeout", "10000");
        props.setProperty(prefix + "maxWaitTimeWhenExhausted", "30");
        props.setProperty(prefix + "clockResolution", ClockType.MSEC.name());
        props.setProperty(prefix + "loadBalancingStrategy", LoadBalancingStrategyType.ROUND_ROBIN.name());
        props.setProperty(prefix + "connectionPool", ConnectionPoolType.TOKEN_AWARE.name());
    
        try {
			NFLibraryManager.initLibrary(PlatformManager.class, props, true, false);
		} catch (NFLibraryException e) {
			LOG.error(e.getMessage());
			Assert.fail();
		}
        Assert.assertEquals("Status Incorrect:", PlatformManager
                .getInstance().getStatus(), Status.INITIALIZED);
                
    }
    
    public static void startKeyspace() {
    	try {
			keyspace = KeyspaceFactory.createKeyspace(CLUSTER_NAME, KEYSPACE_NAME);
		} catch (ConnectionException e) {
			LOG.error(e.getMessage());
			Assert.fail();
		}
    }   
*/
}

package com.netflix.astyanax.cql.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.cql.test.ClusterConfiguration.Driver;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class CqlKeyspaceImplTests extends KeyspaceTests {

	private static final Driver driver = Driver.JAVA_DRIVER;
	//private static final Driver driver = Driver.THRIFT;
	
	@BeforeClass
	public static void init() throws Exception {
		ClusterConfiguration.setDriver(driver);
		initContext();
	}
	
	@Test
	public void runAllTests() throws Exception {

		createKeyspaceUsingOptionsSimpleStrategy();
		createKeyspaceUsingOptionsNetworkTopologyStrategy();
		createKeyspaceAndCFsUsingUsingOptions();
		createKeyspaceAndCFsDirectly();
	}
	
	
	public void createKeyspaceUsingOptionsSimpleStrategy() throws Exception {
		
		String keyspaceName = "AstyanaxTestKeyspaceUsingOptionsSimpleStrategy".toLowerCase();
				
		AstyanaxContext<Keyspace> context = AstyanaxContextFactory.getKeyspace(keyspaceName);
		
    	context.start();
        keyspace = context.getClient();

		Map<String, Object> options = ImmutableMap.<String, Object>builder()
										.put("strategy_options", ImmutableMap.<String, Object>builder()
																	.put("replication_factor", "1")
																	.build())
									    .put("strategy_class",     "SimpleStrategy")
									    .build();
        
		keyspace.createKeyspace(options);
		
		Thread.sleep(1000);
		
		KeyspaceDefinition ksDef = keyspace.describeKeyspace();
		
		Assert.assertEquals(keyspaceName, ksDef.getName());
		Assert.assertTrue(ksDef.getStrategyClass().contains("SimpleStrategy"));

		Properties properties = ksDef.getProperties();
		Assert.assertEquals(keyspaceName, properties.getProperty("name"));
		
		Assert.assertEquals("true", properties.get("durable_writes"));
		
		String strategyClass = properties.getProperty("strategy_class");
		if (strategyClass == null) {
			strategyClass = properties.getProperty("replication.class");
		}
		Assert.assertTrue(ksDef.getStrategyClass().contains("SimpleStrategy"));
		
		Map<String, String> strategyOptions = ksDef.getStrategyOptions();
		Assert.assertEquals("1", strategyOptions.get("replication_factor"));
		
		keyspace.dropKeyspace();
	}
	
	public void createKeyspaceUsingOptionsNetworkTopologyStrategy() throws Exception {
		
		String keyspaceName = "AstyanaxTestKeyspaceUsingOptionsNetworkStrategy".toLowerCase();
				
		AstyanaxContext<Keyspace> context = AstyanaxContextFactory.getKeyspace(keyspaceName);
		
    	context.start();
        keyspace = context.getClient();

		Map<String, Object> options = ImmutableMap.<String, Object>builder()
			    						.put("strategy_options", ImmutableMap.<String, Object>builder()
			    												.put("us-east", "3")
			    												.put("eu-west", "3")
			    												.build())
			    						.put("strategy_class",     "NetworkTopologyStrategy")
			    						.build();
        
		keyspace.createKeyspace(options);
		
		Thread.sleep(1000);
		
		KeyspaceDefinition ksDef = keyspace.describeKeyspace();
		
		Assert.assertEquals(keyspaceName, ksDef.getName());
		Assert.assertTrue(ksDef.getStrategyClass().contains("NetworkTopologyStrategy"));

		Properties properties = ksDef.getProperties();
		Assert.assertEquals(keyspaceName, properties.getProperty("name"));
		
		Assert.assertEquals("true", properties.get("durable_writes"));
		
		String strategyClass = properties.getProperty("strategy_class");
		if (strategyClass == null) {
			strategyClass = properties.getProperty("replication.class");
		}
		Assert.assertTrue(ksDef.getStrategyClass().contains("NetworkTopologyStrategy"));
		
		Map<String, String> strategyOptions = ksDef.getStrategyOptions();
		Assert.assertEquals("3", strategyOptions.get("us-east"));
		Assert.assertEquals("3", strategyOptions.get("eu-west"));
		
		keyspace.dropKeyspace();
	}
	
	public void createKeyspaceAndCFsUsingUsingOptions() throws Exception {
		
		String keyspaceName = "AstyanaxTestKeyspaceAndCFsUsingOptions".toLowerCase();
				
		AstyanaxContext<Keyspace> context = AstyanaxContextFactory.getKeyspace(keyspaceName);
		
    	context.start();
        keyspace = context.getClient();

		Map<String, Object> options = ImmutableMap.<String, Object>builder()
				.put("strategy_options", ImmutableMap.<String, Object>builder()
											.put("replication_factor", "1")
											.build())
			    .put("strategy_class",     "SimpleStrategy")
			    .build();
        
		
		Map<ColumnFamily, Map<String, Object>> cfs = ImmutableMap.<ColumnFamily, Map<String, Object>>builder()
				.put(new ColumnFamily<String, String>("testcf1", StringSerializer.get(), StringSerializer.get()), 
						ImmutableMap.<String, Object>builder()
						.put("bloom_filter_fp_chance", 0.01)
						.build())
				.put(new ColumnFamily<Long, String>("testcf2", LongSerializer.get(), StringSerializer.get()), 
						ImmutableMap.<String, Object>builder()
						.put("read_repair_chance", 0.2)
						.put("bloom_filter_fp_chance", 0.01)
						.build())
				.build();
		
		keyspace.createKeyspace(options, cfs);
		
		Thread.sleep(1000);
		
		KeyspaceDefinition ksDef = keyspace.describeKeyspace();
		
		Assert.assertEquals(keyspaceName, ksDef.getName());
		Assert.assertTrue(ksDef.getStrategyClass().contains("SimpleStrategy"));

		Properties properties = ksDef.getProperties();
		Assert.assertEquals(keyspaceName, properties.getProperty("name"));
		
		Assert.assertEquals("true", properties.get("durable_writes"));
		
		String strategyClass = properties.getProperty("strategy_class");
		if (strategyClass == null) {
			strategyClass = properties.getProperty("replication.class");
		}
		Assert.assertTrue(ksDef.getStrategyClass().contains("SimpleStrategy"));
		
		Map<String, String> strategyOptions = ksDef.getStrategyOptions();
		Assert.assertEquals("1", strategyOptions.get("replication_factor"));

		Properties cfProps = keyspace.getColumnFamilyProperties("testcf1");

		Assert.assertEquals("0.1", String.valueOf(cfProps.get("read_repair_chance")));
		Assert.assertEquals("0.01", String.valueOf(cfProps.get("bloom_filter_fp_chance")));
		Assert.assertEquals("KEYS_ONLY", String.valueOf(cfProps.get("caching")));
		Assert.assertEquals("4", String.valueOf(cfProps.get("min_compaction_threshold")));
		Assert.assertEquals("32", String.valueOf(cfProps.get("max_compaction_threshold")));

		cfProps = keyspace.getColumnFamilyProperties("testcf2");

		Assert.assertEquals("0.2", String.valueOf(cfProps.get("read_repair_chance")));
		Assert.assertEquals("0.01", String.valueOf(cfProps.get("bloom_filter_fp_chance")));
		Assert.assertEquals("KEYS_ONLY", String.valueOf(cfProps.get("caching")));
		Assert.assertEquals("4", String.valueOf(cfProps.get("min_compaction_threshold")));
		Assert.assertEquals("32", String.valueOf(cfProps.get("max_compaction_threshold")));

		keyspace.dropKeyspace();
	}

	public void createKeyspaceAndCFsDirectly() throws Exception {
		
		String keyspaceName = "AstyanaxTestKeyspaceAndCFsDirect".toLowerCase();
				
		AstyanaxContext<Keyspace> context = AstyanaxContextFactory.getKeyspace(keyspaceName);
		
    	context.start();
        keyspace = context.getClient();

		Map<String, Object> ksOptions = ImmutableMap.<String, Object>builder()
				.put("strategy_options", ImmutableMap.<String, Object>builder()
											.put("replication_factor", "1")
											.build())
			    .put("strategy_class",     "SimpleStrategy")
			    .build();
        
		
		keyspace.createKeyspace(ksOptions);
		
		ColumnFamily<String, String> cf1 = new ColumnFamily<String, String>("testcf1", StringSerializer.get(), StringSerializer.get());
		Map<String, Object> options1 = ImmutableMap.<String, Object>builder()
				.put("read_repair_chance", 0.2)
				.put("bloom_filter_fp_chance", 0.01)
				.build();
		
		keyspace.createColumnFamily(cf1, options1);

		Map<String, Object> options2 = new HashMap<String, Object>();
		options2.put("name", "testcf2");
		options2.put("read_repair_chance", 0.4);
		options2.put("bloom_filter_fp_chance", 0.01);

		keyspace.createColumnFamily(options2);
		
		Thread.sleep(1000);
		
		KeyspaceDefinition ksDef = keyspace.describeKeyspace();
		
		Properties cfProps = keyspace.getColumnFamilyProperties("testcf1");

		Assert.assertEquals("0.2", String.valueOf(cfProps.get("read_repair_chance")));
		Assert.assertEquals("0.01", String.valueOf(cfProps.get("bloom_filter_fp_chance")));
		Assert.assertEquals("KEYS_ONLY", String.valueOf(cfProps.get("caching")));
		Assert.assertEquals("4", String.valueOf(cfProps.get("min_compaction_threshold")));
		Assert.assertEquals("32", String.valueOf(cfProps.get("max_compaction_threshold")));

		cfProps = keyspace.getColumnFamilyProperties("testcf2");

		Assert.assertEquals("0.4", String.valueOf(cfProps.get("read_repair_chance")));
		Assert.assertEquals("0.01", String.valueOf(cfProps.get("bloom_filter_fp_chance")));
		Assert.assertEquals("KEYS_ONLY", String.valueOf(cfProps.get("caching")));
		Assert.assertEquals("4", String.valueOf(cfProps.get("min_compaction_threshold")));
		Assert.assertEquals("32", String.valueOf(cfProps.get("max_compaction_threshold")));
		
		ColumnFamilyDefinition cfDef = ksDef.getColumnFamily("testcf1");
		Assert.assertEquals("testcf1", cfDef.getName());
		Assert.assertEquals(0.2, cfDef.getReadRepairChance());
		Assert.assertEquals("KEYS_ONLY", cfDef.getCaching());
		Assert.assertTrue(32 == cfDef.getMaxCompactionThreshold());
		Assert.assertTrue(4 == cfDef.getMinCompactionThreshold());
		Assert.assertEquals(0.01, cfDef.getBloomFilterFpChance());

		cfDef = ksDef.getColumnFamily("testcf2");
		Assert.assertEquals("testcf2", cfDef.getName());
		Assert.assertEquals(0.4, cfDef.getReadRepairChance());
		Assert.assertEquals("KEYS_ONLY", cfDef.getCaching());
		Assert.assertTrue(32 == cfDef.getMaxCompactionThreshold());
		Assert.assertTrue(4 == cfDef.getMinCompactionThreshold());
		Assert.assertEquals(0.01, cfDef.getBloomFilterFpChance());

		List<ColumnFamilyDefinition> cfDefs = ksDef.getColumnFamilyList();
		Assert.assertTrue(2 == cfDefs.size());
		
		cfDef = cfDefs.get(0);
		Assert.assertEquals("testcf1", cfDef.getName());
		Assert.assertEquals(0.2, cfDef.getReadRepairChance());
		Assert.assertEquals("KEYS_ONLY", cfDef.getCaching());
		Assert.assertTrue(32 == cfDef.getMaxCompactionThreshold());
		Assert.assertTrue(4 == cfDef.getMinCompactionThreshold());
		Assert.assertEquals(0.01, cfDef.getBloomFilterFpChance());

		cfDef = cfDefs.get(1);
		Assert.assertEquals("testcf2", cfDef.getName());
		Assert.assertEquals(0.4, cfDef.getReadRepairChance());
		Assert.assertEquals("KEYS_ONLY", cfDef.getCaching());
		Assert.assertTrue(32 == cfDef.getMaxCompactionThreshold());
		Assert.assertTrue(4 == cfDef.getMinCompactionThreshold());
		Assert.assertEquals(0.01, cfDef.getBloomFilterFpChance());

		keyspace.dropKeyspace();
	}

}
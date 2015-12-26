package com.netflix.astyanax.cql.test;

import java.util.Date;
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
import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.cql.test.utils.AstyanaxContextFactory;
import com.netflix.astyanax.cql.test.utils.ClusterConfiguration;
import com.netflix.astyanax.cql.test.utils.ClusterConfiguration.Driver;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class SchemaTests extends KeyspaceTests {

	@BeforeClass
	public static void init() throws Exception {
		initContext();
	}
	
	@Test
	public void createKeyspaceUsingOptions() throws Exception {
		
		String keyspaceName = "AstyanaxTestKeyspaceUsingOptions".toLowerCase();
				
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
		
		verifyKeyspacePropertiesForSimpleStrategy(keyspaceName, ksDef);
		
		keyspace.dropKeyspace();
		
		/** NETWORK TOPOLOGY */ 
		keyspaceName = "AstyanaxTestKeyspaceUsingOptions2".toLowerCase();
		
		context = AstyanaxContextFactory.getKeyspace(keyspaceName);
		context.start();
        keyspace = context.getClient();

		options = ImmutableMap.<String, Object>builder()
			    						.put("strategy_options", ImmutableMap.<String, Object>builder()
			    												.put("us-east", "3")
			    												.put("eu-west", "3")
			    												.build())
			    						.put("strategy_class",     "NetworkTopologyStrategy")
			    						.build();
        
		keyspace.createKeyspace(options);
		
		Thread.sleep(1000);
		
		ksDef = keyspace.describeKeyspace();
		verifyKeyspacePropertiesForNetworkTopology(keyspaceName, ksDef);
		
		keyspace.dropKeyspace();
	}
	
	@Test
	public void createKeyspaceUsingProperties() throws Exception {
		
		/** SIMPLE STRATEGY */
		String keyspaceName = "AstyanaxTestKeyspaceUsingProperties".toLowerCase();
				
		AstyanaxContext<Keyspace> context = AstyanaxContextFactory.getKeyspace(keyspaceName);
		
    	context.start();
        keyspace = context.getClient();

        Properties props = new Properties();
        props.setProperty("strategy_options.replication_factor", "1");
        props.setProperty("strategy_class", "SimpleStrategy");

        keyspace.createKeyspace(props);
		Thread.sleep(1000);
		
		KeyspaceDefinition ksDef = keyspace.describeKeyspace();
		verifyKeyspacePropertiesForSimpleStrategy(keyspaceName, ksDef);
		
		keyspace.dropKeyspace();
		
		/** NETWORK TOPOLOGY STRATEGY */
		keyspaceName = "AstyanaxTestKeyspaceUsingProperties2".toLowerCase();
		
		context = AstyanaxContextFactory.getKeyspace(keyspaceName);
		
    	context.start();
        keyspace = context.getClient();

        props = new Properties();

        props.setProperty("strategy_options.us-east", "3");
        props.setProperty("strategy_options.eu-west", "3");
        props.setProperty("strategy_class", "NetworkTopologyStrategy");

        keyspace.createKeyspace(props);
		Thread.sleep(1000);
		
		ksDef = keyspace.describeKeyspace();
		verifyKeyspacePropertiesForNetworkTopology(keyspaceName, ksDef);
		
		keyspace.dropKeyspace();
	}
	
	private void verifyKeyspacePropertiesForSimpleStrategy(String keyspaceName, KeyspaceDefinition ksDef) throws Exception {
		
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
	}
	

	private void verifyKeyspacePropertiesForNetworkTopology(String keyspaceName, KeyspaceDefinition ksDef) throws Exception {
		
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
	}
	
	@Test
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
		verifyKeyspacePropertiesForSimpleStrategy(keyspaceName, ksDef);
		
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

	@Test
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
		verifyKeyspacePropertiesForSimpleStrategy(keyspaceName, ksDef);
		
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
	
	@Test
	public void createKeyspaceWithCompositeCF() throws Exception {
		
		// Annotated composite class
		class Population {
		  @Component(ordinal=0) String country;
		  @Component(ordinal=1) String state;
		  @Component(ordinal=2) String city;
		  @Component(ordinal=3) Integer zip;
		  @Component(ordinal=3) Date district;
		  // Must have public default constructor
		  public Population() {
		  }
		}

		AnnotatedCompositeSerializer<Population> compSerializer = new AnnotatedCompositeSerializer<Population>(Population.class);
		ColumnFamily<String, Population> CF_POPULATION = 
				new ColumnFamily<String, Population>("population", StringSerializer.get(), compSerializer);
		
		String keyspaceName = "AstyanaxTestKeyspaceCompositeCFs".toLowerCase();
		
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

		KeyspaceDefinition ksDef = keyspace.describeKeyspace();
		Assert.assertEquals(keyspaceName, ksDef.getName());

		keyspace.createColumnFamily(CF_POPULATION, ImmutableMap.<String, Object>builder()
        .put("default_validation_class", "UTF8Type")
        .put("key_validation_class",     "UTF8Type")
        .put("comparator_type",          "CompositeType(UTF8Type, UTF8Type, UTF8Type, Int32Type, DateType)")
        .build());

		if (ClusterConfiguration.getDriver().equals(Driver.JAVA_DRIVER)) {
			List<ColumnFamilyDefinition> list = ksDef.getColumnFamilyList();
			Assert.assertTrue(1 == list.size());
		
			ColumnFamilyDefinition cfDef = list.get(0);
			Assert.assertEquals("population", cfDef.getName());
		
			List<ColumnDefinition> colDefs = cfDef.getColumnDefinitionList();
			Assert.assertTrue(7 == colDefs.size());
		
			for (int i=1; i<=5; i++) {
				ColumnDefinition colDef = colDefs.get(i-1);
				Assert.assertEquals("column" + i, colDef.getName());
				Assert.assertNotNull(colDef.getValidationClass());
			}
			ColumnDefinition colDef = colDefs.get(6);
			Assert.assertEquals("value", colDef.getName());
			Assert.assertNotNull(colDef.getValidationClass());
			
			cfDef = ksDef.getColumnFamily("population");
			Assert.assertEquals("population", cfDef.getName());
			
			colDefs = cfDef.getColumnDefinitionList();
			Assert.assertTrue(7 == colDefs.size());
		
			for (int i=1; i<=5; i++) {
				colDef = colDefs.get(i-1);
				Assert.assertEquals("column" + i, colDef.getName());
				Assert.assertNotNull(colDef.getValidationClass());
			}
			colDef = colDefs.get(6);
			Assert.assertEquals("value", colDef.getName());
			Assert.assertNotNull(colDef.getValidationClass());
		}

		keyspace.dropKeyspace();
	}
	
	@Test
	public void alterKeyspaceOptions() throws Exception {
		
		String keyspaceName = "AstyanaxTestKeyspaceAlterOptions".toLowerCase();

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
		verifyKeyspacePropertiesForSimpleStrategy(keyspaceName, ksDef);
		
		keyspace.updateKeyspace(ImmutableMap.<String, Object>builder()
										.put("strategy_options", ImmutableMap.<String, Object>builder()
																	.put("replication_factor", "2")
																	.build())
									    .put("strategy_class",     "SimpleStrategy")
									    .build());
		ksDef = keyspace.describeKeyspace();
		Assert.assertEquals("2", ksDef.getStrategyOptions().get("replication_factor"));
		
		keyspace.dropKeyspace();
		
		/** NETWORK TOPOLOGY */ 
		keyspaceName = "AstyanaxTestKeyspaceAlterOptions2".toLowerCase();
		
		context = AstyanaxContextFactory.getKeyspace(keyspaceName);
		context.start();
        keyspace = context.getClient();

		options = ImmutableMap.<String, Object>builder()
			    						.put("strategy_options", ImmutableMap.<String, Object>builder()
			    												.put("us-east", "3")
			    												.put("eu-west", "3")
			    												.build())
			    						.put("strategy_class",     "NetworkTopologyStrategy")
			    						.build();
        
		keyspace.createKeyspace(options);
		Thread.sleep(1000);
		
		KeyspaceDefinition ksDef2 = keyspace.describeKeyspace();
		verifyKeyspacePropertiesForNetworkTopology(keyspaceName, ksDef2);

		options = ImmutableMap.<String, Object>builder()
				.put("strategy_options", ImmutableMap.<String, Object>builder()
										.put("us-east", "2")
										.put("eu-west", "2")
										.build())
				.put("strategy_class",     "NetworkTopologyStrategy")
				.build();

		keyspace.updateKeyspace(options);
		ksDef2 = keyspace.describeKeyspace();
		
		System.out.println(ksDef2.getStrategyOptions());
		Assert.assertEquals("2", ksDef2.getStrategyOptions().get("us-east"));
		Assert.assertEquals("2", ksDef2.getStrategyOptions().get("eu-west"));

		keyspace.dropKeyspace();
	}
	
	@Test
	public void alterCFOptions() throws Exception {
		
		String keyspaceName = "AstyanaxTestKeyspaceAlterCFOptions".toLowerCase();
		
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
		
		ColumnFamily<String, String> cf = new ColumnFamily<String, String>("testaltercf1", StringSerializer.get(), StringSerializer.get());
		keyspace.createColumnFamily(cf, null);
		
		Assert.assertEquals(0.1, keyspace.getColumnFamilyProperties("testaltercf1").get("read_repair_chance"));
		
		keyspace.updateColumnFamily(cf, ImmutableMap.<String, Object>builder()
										.put("read_repair_chance", 0.2)
										.build());
		Assert.assertEquals(0.2, keyspace.getColumnFamilyProperties("testaltercf1").get("read_repair_chance"));

		keyspace.dropKeyspace();
	}
	
	@Test
	public void createAndDeleteCF() throws Exception {
		
		String keyspaceName = "AstyanaxTestKeyspaceCreateDeleteCF".toLowerCase();
		
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
		
		ColumnFamily<String, String> cf = new ColumnFamily<String, String>("testcreatedeletecf1", StringSerializer.get(), StringSerializer.get());
		keyspace.createColumnFamily(cf, null);
		Assert.assertEquals(0.1, keyspace.getColumnFamilyProperties("testcreatedeletecf1").get("read_repair_chance"));
		
		keyspace.dropColumnFamily(cf);
		try {
			keyspace.getColumnFamilyProperties("testaltercf1");
			Assert.fail("Should have gotten CF not found ex");
		} catch(RuntimeException e) {
			
		} finally {
			keyspace.dropKeyspace();
		}
	}
	
	@Test
	public void createAndDeleteKeyspace() throws Exception {
		
		String keyspaceName = "AstyanaxTestKeyspaceCreateDeleteKS".toLowerCase();

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
		Assert.assertTrue(ksDef.getStrategyClass().contains("SimpleStrategy"));

		keyspace.dropKeyspace();
		try {
			keyspace.describeKeyspace();
			Assert.fail("Should have gotten KS not found ex");
		} catch(RuntimeException e) {
			
		}
	}

	@Test
	public void keyspaceDescribePartitioner() throws Exception {
		
		String keyspaceName = "AstyanaxTestKeyspaceDescribeRing".toLowerCase();

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

		String partitioner = keyspace.describePartitioner();
		Assert.assertNotNull(partitioner);
		
		keyspace.dropKeyspace();
	}
}
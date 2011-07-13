package com.netflix.astyanax.thrift;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.MillisecondsClock;
import com.netflix.astyanax.mock.MockConstants;
import com.netflix.astyanax.mock.MockEmbeddedCassandra;
import com.netflix.astyanax.model.AbstractComposite;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.ColumnType;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.model.KeySlice;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.PrefixedSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.cassandra.config.ConnectionPoolType;
import com.netflix.cassandra.config.FailoverStrategyType;
import com.netflix.cassandra.config.LoadBalancingStrategyType;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ThriftKeyspaceImplTest {

    private static final Logger LOG = Logger.getLogger(ThriftKeyspaceImplTest.class);
    
	private static MockEmbeddedCassandra cassandra;
	
	private static Keyspace keyspace;
	
	@BeforeClass 
	public static void setup() throws Exception {
		cassandra = new MockEmbeddedCassandra();
		cassandra.setup();
		createKeyspace();
	}
	
	@AfterClass
	public static void teardown() {
		if (cassandra != null) 
			cassandra.teardown();
	}
	
	public static void createKeyspace() throws Exception {
    	ConnectionPoolConfigurationImpl config 
    		= new ConnectionPoolConfigurationImpl(MockConstants.CLUSTER_NAME, MockConstants.KEYSPACE_NAME);

    	config.setSeeds("127.0.0.1:"+MockConstants.PORT);
    	config.setPort(MockConstants.PORT);
    	config.setSocketTimeout(30000);
    	config.setMaxTimeoutWhenExhausted(30);
    	config.setClock(new MillisecondsClock());
    	config.setLoadBlancingStrategyFactory(LoadBalancingStrategyType.ROUND_ROBIN);
    	// config.setNodeDiscoveryFactory(NodeDiscoveryType.TOKEN_AWARE);
    	config.setConnectionPoolFactory(ConnectionPoolType.ROUND_ROBIN);
    	config.setFailoverStrategyFactory(FailoverStrategyType.ALL);
    	
    	LOG.info(config);
    	keyspace = new ThriftKeyspaceImpl(config);
    	keyspace.start();
	}
	
	@Test
    @Ignore
	public void testComposite() {
		Composite c = new Composite();
		c.addComponent("String1", StringSerializer.get())
		 .addComponent(123, IntegerSerializer.get());
		
		MutationBatch m = keyspace.prepareMutationBatch();
		m.withRow(MockConstants.CF_COMPOSITE, "Key1")
			.putColumn(c, 123, null);
		
		try {
			m.execute();
		} catch (ConnectionException e) {
			Assert.fail();
		}
		
		try {
			OperationResult<Column<Composite>> result = keyspace.prepareQuery(MockConstants.CF_COMPOSITE)
				.getKey("Key1")
				.getColumn(c)
				.execute();
			
			Assert.assertEquals(123, result.getResult().getIntegerValue());
		} catch (ConnectionException e) {
			Assert.fail();
		}
	}
	
	@Test
	@Ignore
	public void testIndexQuery() {
		OperationResult<Rows<String, String>> result;
		try {
			LOG.info("************************************************** prepareGetMultiRowIndexQuery: ");
			
			result = keyspace.prepareQuery(MockConstants.CF_STANDARD1)
				.searchWithIndex()
					.setStartKey("")
					.addExpression()
						.whereColumn("Index1").equals().value(26)
					.execute();
			
			for (Row<String, String> row : result.getResult()) {
				LOG.info("RowKey is" + row.getKey());
				for (Column<String> column : row.getColumns()) {
					LOG.info("  Column: " + column.getName() + "=" + column.getIntegerValue());
				}
			}
			
			LOG.info("************************************************** Index query: " + result.getResult().size());
			
		} catch (ConnectionException e) {
			LOG.error("**************************************************" + e.getMessage());
			e.printStackTrace();
			Assert.fail();
		} catch (Exception e) {
			LOG.error("**************************************************" + e.getMessage());
			Assert.fail();
		}
	}

	@Test
    @Ignore
    public void testIncrementCounter() {
    	
		MutationBatch m = keyspace.prepareMutationBatch();
		m.withRow(MockConstants.CF_COUNTER1, "CounterRow1")
			.incrementCounterColumn("MyCounter", 100);
		try {
			m.execute();
		} catch (ConnectionException e) {
			LOG.error(e.getMessage());
			Assert.fail();
		}
		
		Column<String> column = getColumnValue(MockConstants.CF_COUNTER1, "CounterRow1", "MyCounter");
		Assert.assertNotNull(column);
		Assert.assertEquals(column.getLongValue(), 100);

		m = keyspace.prepareMutationBatch();
		m.withRow(MockConstants.CF_COUNTER1, "CounterRow1")
			.incrementCounterColumn("MyCounter", 100);
		try {
			m.execute();
		} catch (ConnectionException e) {
			LOG.error(e.getMessage());
			Assert.fail();
		}
		
		column = getColumnValue(MockConstants.CF_COUNTER1, "CounterRow1", "MyCounter");
		Assert.assertNotNull(column);
		Assert.assertEquals(column.getLongValue(), 200);
    }

    @Test
    @Ignore
    public void testGetSingleColumn() {
		Column<String> column = getColumnValue(MockConstants.CF_STANDARD1, "A", "a");
		Assert.assertNotNull(column);
		Assert.assertEquals(1, column.getIntegerValue());
    }
    
    @Test
	@Ignore
    public void testGetSingleColumnNotExists() {
		Column<String> column = getColumnValue(MockConstants.CF_STANDARD1, "A", "ab");
		Assert.assertNull(column);
    }
		
    @Test
	@Ignore
    public void testGetSingleKeyNotExists() {
    	Column<String> column = getColumnValue(MockConstants.CF_STANDARD1, "AA", "ab");
		Assert.assertNull(column);
    }
    
    @Test
    @Ignore
    public void testFunctionalQuery() throws ConnectionException {
    	OperationResult<ColumnList<String>> r1 = keyspace.prepareQuery(MockConstants.CF_STANDARD1)
    		.getKey("A")
    		.execute();
    	Assert.assertEquals(27, r1.getResult().size());

    	/*
    	OperationResult<Rows<String, String>> r2 = keyspace.prepareQuery()
    		.fromColumnFamily(MockConstants.CF_STANDARD1)
    		.selectKeyRange("A", "Z", null, null, 5)
    		.execute();
    		*/
    }
    
    @Test
    @Ignore
    public void testColumnSlice() throws ConnectionException {
    	OperationResult<ColumnList<String>> r1 = keyspace.prepareQuery(MockConstants.CF_STANDARD1)
			.getKey("A")
			.withColumnSlice("a", "b")
			.execute();
    	Assert.assertEquals(2, r1.getResult().size());
    }
    
    @Test
    public void testColumnRangeSlice() throws ConnectionException {
    	OperationResult<ColumnList<String>> r1 = keyspace.prepareQuery(MockConstants.CF_STANDARD1)
			.getKey("A")
			.withColumnRange("a", "b", false, 5)
			.execute();
    	Assert.assertEquals(2, r1.getResult().size());
    	
    	OperationResult<ColumnList<String>> r2 = keyspace.prepareQuery(MockConstants.CF_STANDARD1)
    		.getKey("A")
    		.withColumnRange(null, null, false, 5)
    		.execute();
    	Assert.assertEquals(5, r2.getResult().size());
    	Assert.assertEquals("a", r2.getResult().getColumnByIndex(0).getName());

    	OperationResult<ColumnList<String>> r3 = keyspace.prepareQuery(MockConstants.CF_STANDARD1)
			.getKey("A")
			.withColumnRange(null, null, true, 5)
			.execute();
    	Assert.assertEquals(5, r3.getResult().size());
    	Assert.assertEquals("z", r3.getResult().getColumnByIndex(0).getName());
    }
    
    @Test
    @Ignore
    public void testGetColumnsWithPrefix() throws ConnectionException {
    	OperationResult<ColumnList<String>> r = keyspace.prepareQuery(MockConstants.CF_STANDARD1)
    		.getKey("Prefixes")
    		.withColumnRange("Prefix1_\u00000", "Prefix1_\uffff", false, Integer.MAX_VALUE)
    		.execute();
    	Assert.assertEquals(2, r.getResult().size());
    	Assert.assertEquals("Prefix1_a", r.getResult().getColumnByIndex(0).getName());
    	Assert.assertEquals("Prefix1_b", r.getResult().getColumnByIndex(1).getName());
    }
    
    @Test
	@Ignore
    public void testGetCounters() throws ConnectionException {
    	LOG.info("Starting testGetCounters...");
		
		ColumnPath<String> path = new ColumnPath<String>(StringSerializer.get()).append("TestCounter");
		
		try {
			OperationResult<Column<String>> result = 
				keyspace.prepareGetColumnQuery(MockConstants.CF_COUNTER1, "CounterRow1", path)
					  .execute();
			
			Long count = result.getResult().getLongValue();
			
		}
		catch (NotFoundException e) {
			
		}
		
		OperationResult<Void> incr = keyspace.prepareCounterMutation(MockConstants.CF_COUNTER1, "1", path, 100).execute();
    	LOG.info("... testGetCounters done");
    }
    
	@Test
	@Ignore
	public void testGetSingleKey() {
		try {
			for (char key = 'A'; key <= 'Z'; key++) {
				String keyName = Character.toString(key);
				OperationResult<ColumnList<String>> result = 
					keyspace.prepareGetRowQuery(
							MockConstants.CF_SUPER1, 
							MockConstants.CF_SUPER1.getColumnSerializer(), 
							keyName)
						  .execute();
				
				Assert.assertNotNull(result.getResult());
				
				System.out.printf("%s executed on %s in %d msec size=%d\n", 
						keyName, 
						result.getHost(),
						result.getLatency(),
						result.getResult().size());
			}
		} catch (ConnectionException e) {
			LOG.error(e.getMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	@Ignore
	public void testGetAllKeysRoot() {
    	LOG.info("Starting testGetAllKeysRoot...");
    	
    	try {
			List<String> keys = new ArrayList<String>();
			for (char key = 'A'; key <= 'Z'; key++) {
				String keyName = Character.toString(key);
				keys.add(keyName);
			}
			
			OperationResult<Rows<String, String>> result = 
				keyspace.prepareGetMultiRowQuery(
						MockConstants.CF_SUPER1, 
						MockConstants.CF_SUPER1.getColumnSerializer(), 
						new KeySlice<String>(keys))
				  .execute();
			
			LOG.info("Get " + result.getResult().size() + " keys");
			for (Row<String, String> row : result.getResult()) {
				LOG.info(String.format("%s executed on %s in %d msec size=%d\n", 
						row.getKey(), 
						result.getHost(),
						result.getLatency(),
						row.getColumns().size()));
				for (Column<String> sc : row.getColumns()) {
					LOG.info("  " + sc.getName());
					ColumnList<Integer> subColumns = sc.getSubColumns(IntegerSerializer.get());
					for (Column<Integer> sub : subColumns) {
						LOG.info("    " + sub.getName() + "=" + sub.getStringValue());
					}
				}
			}
			
		} catch (ConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			Assert.fail();
		}
		
    	LOG.info("... testGetAllKeysRoot");
	}
	
	@Test
	@Ignore
	public void testGetColumnSlice() {
    	LOG.info("Starting testGetColumnSlice...");
		try {
			OperationResult<ColumnList<String>> result =
				keyspace.prepareGetRowQuery(
						MockConstants.CF_STANDARD1, 
						MockConstants.CF_STANDARD1.getColumnSerializer(),
						"A")
				  .setColumnSlice(new ColumnSlice<String>("c", "h").setLimit(5))
				  .execute();
			Assert.assertNotNull(result.getResult());
			Assert.assertEquals(5, result.getResult().size());
		} catch (ConnectionException e) {
			Assert.fail(e.getMessage());
		}
		
	}
	
	@Test
	@Ignore
	public void testGetAllKeysPath() {
    	LOG.info("Starting testGetAllKeysPath...");
    	
    	try {
			List<String> keys = new ArrayList<String>();
			for (char key = 'A'; key <= 'Z'; key++) {
				String keyName = Character.toString(key);
				keys.add(keyName);
			}
			
			OperationResult<Rows<String, Integer>> result =
				keyspace.prepareGetMultiRowQuery(
						MockConstants.CF_SUPER1, 
						IntegerSerializer.get(), 
						new KeySlice<String>(keys))
				  .setColumnPath(new ColumnPath<Integer>(IntegerSerializer.get()).append("a", StringSerializer.get()))
				  .execute();
			
			for (Row<String, Integer> row : result.getResult()) {
				System.out.printf("%s executed on %s in %d msec size=%d\n", 
						row.getKey(), 
						result.getHost(),
						result.getLatency(),
						row.getColumns().size());
				for (Column<Integer> column : row.getColumns()) {
					System.out.println("  Column: " + column.getName());
				}
			}
		} catch (ConnectionException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
    	LOG.info("Starting testGetAllKeysPath...");
	}
	
	@Test
	@Ignore
	public void testDeleteMultipleKeys() {
    	LOG.info("Starting testDeleteMultipleKeys...");
    	LOG.info("... testGetAllKeysPath");
		
	}
	
	@Test
	@Ignore
	public void testMutationMerge() {
		MutationBatch m1 = keyspace.prepareMutationBatch();
		MutationBatch m2 = keyspace.prepareMutationBatch();
		MutationBatch m3 = keyspace.prepareMutationBatch();
		MutationBatch m4 = keyspace.prepareMutationBatch();
		MutationBatch m5 = keyspace.prepareMutationBatch();
		
		m1.withRow(MockConstants.CF_STANDARD1, "1")
			.putColumn("1",  "X", null);
		m2.withRow(MockConstants.CF_STANDARD1, "2")
			.putColumn("2",  "X", null)
			.putColumn("3",  "X", null);
		m3.withRow(MockConstants.CF_STANDARD1, "3")
			.putColumn("4",  "X", null)
			.putColumn("5",  "X", null)
			.putColumn("6",  "X", null);
		m4.withRow(MockConstants.CF_STANDARD1, "1")
			.putColumn("7",  "X", null)
			.putColumn("8",  "X", null)
			.putColumn("9",  "X", null)
			.putColumn("10", "X", null);
		
		MutationBatch merged = keyspace.prepareMutationBatch();
		LOG.info(merged);
		Assert.assertEquals(merged.getRowCount(), 0);
		
		merged.mergeShallow(m1);
		LOG.info(merged);
		Assert.assertEquals(merged.getRowCount(), 1);
		
		merged.mergeShallow(m2);
		LOG.info(merged);
		Assert.assertEquals(merged.getRowCount(), 2);
		
		merged.mergeShallow(m3);
		LOG.info(merged);
		Assert.assertEquals(merged.getRowCount(), 3);
		
		merged.mergeShallow(m4);
		LOG.info(merged);
		Assert.assertEquals(merged.getRowCount(), 3);

		merged.mergeShallow(m5);
		LOG.info(merged);
		Assert.assertEquals(merged.getRowCount(), 3);
	}
	
	@Test
	@Ignore
	public void testDelete() {
    	LOG.info("Starting testDelete...");
    	
    	String rowKey = "DeleteMe";
    	
		MutationBatch m = keyspace.prepareMutationBatch();
		m.withRow(MockConstants.CF_STANDARD1, rowKey)
		  .putColumn("Column1", "X", null)
		  .putColumn("Column2", "X", null);
		
		try {
			OperationResult<Void> result = m.execute();
		} catch (ConnectionException e) {
			LOG.error(e);
			Assert.fail();
		}
		
		Assert.assertEquals(getColumnValue(MockConstants.CF_STANDARD1, rowKey, "Column1").getStringValue(), "X");
		Assert.assertTrue(deleteColumn(MockConstants.CF_STANDARD1, rowKey, "Column1"));
		Assert.assertNull(getColumnValue(MockConstants.CF_STANDARD1, rowKey, "Column1"));
		
    	LOG.info("... testDelete");
	}
	
	@Test
	@Ignore
	public void testPrefixedSerializer() {
		ColumnFamily<String, String> cf = new ColumnFamily<String, String>("Standard1", 
				StringSerializer.get(),
				StringSerializer.get(),
				ColumnType.STANDARD);
		
		ColumnFamily<String, String> cf1 = new ColumnFamily<String, String>("Standard1", 
				new PrefixedSerializer<String, String>("Prefix1_", StringSerializer.get(), StringSerializer.get()),
				StringSerializer.get(),
				ColumnType.STANDARD);
		
		ColumnFamily<String, String> cf2 = new ColumnFamily<String, String>("Standard1", 
				new PrefixedSerializer<String, String>("Prefix2_", StringSerializer.get(), StringSerializer.get()),
				StringSerializer.get(),
				ColumnType.STANDARD);
		
		MutationBatch m = keyspace.prepareMutationBatch();
		m.withRow(cf1, "A")
		  .putColumn("Column1", "Value1", null);
		m.withRow(cf2, "A")
		  .putColumn("Column1", "Value2", null);
				
		try {
			m.execute();
		} catch (ConnectionException e) {
			LOG.error(e);
			Assert.fail();
		}
		
		try {
			OperationResult<ColumnList<String>> result = keyspace.prepareQuery(cf)
				.getKey("Prefix1_A")
				.execute();
			Assert.assertEquals(1, result.getResult().size());
			Column<String> c = result.getResult().getColumnByName("Column1");
			Assert.assertEquals("Value1", c.getStringValue());
		} catch (ConnectionException e) {
			LOG.error(e);
			Assert.fail();
		}
		
		try {
			OperationResult<ColumnList<String>> result = keyspace.prepareQuery(cf)
				.getKey("Prefix2_A")
				.execute();
			Assert.assertEquals(1, result.getResult().size());
			Column<String> c = result.getResult().getColumnByName("Column1");
			Assert.assertEquals("Value2", c.getStringValue());
		} catch (ConnectionException e) {
			LOG.error(e);
			Assert.fail();
		}
		
	}
	
	private boolean deleteColumn(ColumnFamily<String, String> cf, String rowKey, String columnName) {
		MutationBatch m = keyspace.prepareMutationBatch();
		m.withRow(cf, rowKey)
		  .deleteColumn(columnName);
		
		try {
			OperationResult<Void> result = m.execute();
			return true;
		} catch (ConnectionException e) {
			LOG.error(e);
			Assert.fail();
			return false;
		}
	}
	
	private Column<String> getColumnValue(ColumnFamily<String, String> cf, String rowKey, String columnName) {
		OperationResult<Column<String>> result;
		try {
			result = keyspace.prepareQuery(cf)
				.getKey(rowKey)
				.getColumn(columnName)
				.execute();
			return result.getResult(); 
		} catch (NotFoundException e) {
			return null;
		} catch (ConnectionException e) {
			LOG.error(e);
			Assert.fail();
			return null;
		}
	}

	private Column<Integer> getColumnValue(ColumnFamily<String, String> cf, String rowKey, String superColumn, Integer columnName) {
		OperationResult<Column<Integer>> result;
		try {
			result = keyspace.prepareGetColumnQuery(cf, rowKey, new ColumnPath<Integer>(IntegerSerializer.get()).append(superColumn).append(columnName))
			  .execute();
			return result.getResult(); 
		} catch (NotFoundException e) {
			return null;
		} catch (ConnectionException e) {
			LOG.error(e);
			Assert.fail();
			return null;
		}
	}
}

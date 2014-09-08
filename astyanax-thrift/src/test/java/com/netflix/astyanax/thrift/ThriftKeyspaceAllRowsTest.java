package com.netflix.astyanax.thrift;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.RowCallback;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxCheckpointManager;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.CheckpointManager;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

public class ThriftKeyspaceAllRowsTest {
    
    private static Logger LOG = LoggerFactory.getLogger(ThriftKeyspaceAllRowsTest.class);

    private static Keyspace                  keyspace;
    private static AstyanaxContext<Keyspace> keyspaceContext;

    private static String TEST_CLUSTER_NAME  = "cass_sandbox";
    private static String TEST_KEYSPACE_NAME = "AstyanaxUnitTests";
    private static final String SEEDS = "localhost:9160";
    private static final long   CASSANDRA_WAIT_TIME = 3000;
    private static final long   LOTS_OF_ROWS_COUNT = 1000;
    
    public static ColumnFamily<Long, String> CF_ALL_ROWS = 
            ColumnFamily.newColumnFamily("AllRows1",       LongSerializer.get(), StringSerializer.get());

    public static ColumnFamily<Long, String> CF_ALL_ROWS_TOMBSTONE = 
            ColumnFamily.newColumnFamily("AllRowsTombstone1",       LongSerializer.get(), StringSerializer.get());

    public static ColumnFamily<Long, String> CF_LOTS_OF_ROWS = 
            new ColumnFamily<Long, String>("LotsOfRows1",       LongSerializer.get(), StringSerializer.get());

    public static ColumnFamily<Long, String> CF_CHECKPOINTS = 
            new ColumnFamily<Long, String>("Checkpoints",       LongSerializer.get(), StringSerializer.get());

    @BeforeClass
    public static void setup() throws Exception {
        System.out.println("TESTING THRIFT KEYSPACE");

        SingletonEmbeddedCassandra.getInstance();
        
        Thread.sleep(CASSANDRA_WAIT_TIME);
        
        createKeyspace();
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (keyspaceContext != null)
            keyspaceContext.shutdown();
        
        Thread.sleep(CASSANDRA_WAIT_TIME);
    }

    public static void createKeyspace() throws Exception {
        keyspaceContext = new AstyanaxContext.Builder()
                .forCluster(TEST_CLUSTER_NAME)
                .forKeyspace(TEST_KEYSPACE_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
                                .setDiscoveryDelayInSeconds(60000))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME
                                + "_" + TEST_KEYSPACE_NAME)
                                .setSocketTimeout(30000)
                                .setMaxTimeoutWhenExhausted(2000)
                                .setMaxConnsPerHost(20)
                                .setInitConnsPerHost(10)
                                .setSeeds(SEEDS)
                                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        keyspaceContext.start();
        
        keyspace = keyspaceContext.getEntity();
        
        try {
            keyspace.dropKeyspace();
        }
        catch (Exception e) {
            LOG.info(e.getMessage());
        }
        
        keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                        .put("replication_factor", "1")
                        .build())
                .put("strategy_class",     "SimpleStrategy")
                .build()
                );
        
       
        keyspace.createColumnFamily(CF_ALL_ROWS,            null);
        keyspace.createColumnFamily(CF_ALL_ROWS_TOMBSTONE,  null);
        keyspace.createColumnFamily(CF_LOTS_OF_ROWS,        null);
        keyspace.createColumnFamily(CF_CHECKPOINTS,         null);
        
        KeyspaceDefinition ki = keyspaceContext.getEntity().describeKeyspace();
        System.out.println("Describe Keyspace: " + ki.getName());

        MutationBatch m;
        try {
            m = keyspace.prepareMutationBatch();
            // Add 10 rows
            for (long i = 0; i < 10; i++) {
                m.withRow(CF_ALL_ROWS, i)
                    .putColumn("A", 1)
                    .putColumn("B", 1)
                    ;
            }
            // Add 10 rows
            for (long i = 10; i < 20; i++) {
                m.withRow(CF_ALL_ROWS, i)
                    .putColumn("B", 1)
                    .putColumn("C", 1)
                    ;
            }
            // Add 10 rows
            for (long i = 20; i < 30; i++) {
                m.withRow(CF_ALL_ROWS, i)
                    .putColumn("B", 1)
                    .putColumn("C", 1)
                    ;
            }
            for (long i = 0; i < 100; i++) {
                m.withRow(CF_ALL_ROWS_TOMBSTONE, i)
                 .delete()
                ;
            }
            m.execute();
            
            m = keyspace.prepareMutationBatch();
            // Delete 7
            for (long i = 0; i < 20; i += 3) {
                m.withRow(CF_ALL_ROWS, i)
                    .delete();
            }
            // Delete 10
            for (long i = 20; i < 30; i ++ ) {
                m.withRow(CF_ALL_ROWS, i)
                    .delete();
            }
            
            // CF_ALL_ROWS should have 13 rows + 17 tombstones
            
            m.execute();            
            
            // Add 10,000 rows
            m = keyspace.prepareMutationBatch();
            for (long i = 0; i < LOTS_OF_ROWS_COUNT; i++) {
                m.withRow(CF_LOTS_OF_ROWS, i).putColumn("DATA", "TEST" + i);
            }
            m.execute();
            
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }
    
    public static <K, C> Set<K> getKeySet(Rows<K, C> rows) {
        Set<K> set = new TreeSet<K>();
        for (Row<K, C> row : rows) {
            if (set.contains(row.getKey())) 
                Assert.fail("Duplicate key found : " + row.getKey());
            LOG.info("Row: " + row.getKey());
            set.add(row.getKey());
        }
        return set;
    }
    
    @Test
    public void testGetAll() {
        try {
            OperationResult<Rows<Long, String>> rows = keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .setRowLimit(5)
                .setExceptionCallback(new ExceptionCallback() {
                    @Override
                    public boolean onException(ConnectionException e) {
                        Assert.fail(e.getMessage());
                        return true;
                    }
                })
                .execute();
            
            for (Row<Long, String> row : rows.getResult()) {
                LOG.info("Row: " + row.getKey() + " count=" + row.getColumns().size());
            }
            Set<Long> set = getKeySet(rows.getResult());
            LOG.info(set.toString());
            Assert.assertEquals(13,  set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
    }
    
   @Test
    public void testGetAllDefaults() {
        try {
            OperationResult<Rows<Long, String>> rows = keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
//                  .setRowLimit(5)
                .setExceptionCallback(new ExceptionCallback() {
                    @Override
                    public boolean onException(ConnectionException e) {
                        Assert.fail(e.getMessage());
                        return true;
                    }
                })
                .execute();
            
            Set<Long> set = getKeySet(rows.getResult());
            LOG.info(set.toString());
            Assert.assertEquals(13,  set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
    }
    
    @Test
    public void testGetAllWithTombstones() {
        try {
            OperationResult<Rows<Long, String>> rows = keyspace.prepareQuery(CF_ALL_ROWS_TOMBSTONE)
                .getAllRows()
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .execute();
            
            Set<Long> set = getKeySet(rows.getResult());
            LOG.info("All columns row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(0, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            OperationResult<Rows<Long, String>> rows = keyspace.prepareQuery(CF_ALL_ROWS_TOMBSTONE)
                .getAllRows()
                .setRepeatLastToken(false)
                .setIncludeEmptyRows(true)
                .setRowLimit(5)
                .execute();
            
            Set<Long> set = getKeySet(rows.getResult());
            LOG.info("All columns row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(100, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            OperationResult<Rows<Long, String>> rows = keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .execute();
            
            Set<Long> set = getKeySet(rows.getResult());
            LOG.info("All columns row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(13, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            OperationResult<Rows<Long, String>> rows = keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .withColumnSlice("A")
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .execute();
            
            Set<Long> set = getKeySet(rows.getResult());
            LOG.info("Column='A' Row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(6, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            OperationResult<Rows<Long, String>> rows = keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .withColumnSlice("B")
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .execute();
            
            Set<Long> set = getKeySet(rows.getResult());
            LOG.info("Column='B' Row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(13, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            OperationResult<Rows<Long, String>> rows = keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .withColumnRange(new RangeBuilder().setLimit(1).build())
                .setRowLimit(5)
                .setRepeatLastToken(false)
                .execute();
            
            Set<Long> set = getKeySet(rows.getResult());
            LOG.info("Limit 1 row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(13, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            OperationResult<Rows<Long, String>> rows = keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .withColumnRange(new RangeBuilder().setLimit(0).build())
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .execute();
            
            Set<Long> set = getKeySet(rows.getResult());
            LOG.info("Limit 0 row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(30, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            OperationResult<Rows<Long, String>> rows = keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .execute();
            
            Set<Long> set = getKeySet(rows.getResult());
            LOG.info("All columns row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(13, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            OperationResult<Rows<Long, String>> rows = keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .setIncludeEmptyRows(true)
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .execute();
            
            Set<Long> set = getKeySet(rows.getResult());
            LOG.info("IncludeEmpty Row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(30, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
    }
    
    public static class ToKeySetCallback<K,C> implements RowCallback<K, C> {
        private Set<K> set = new TreeSet<K>();
        
        @Override
        public synchronized void success(Rows<K, C> rows) {
            set.addAll(getKeySet(rows));
        }

        @Override
        public boolean failure(ConnectionException e) {
            // TODO Auto-generated method stub
            return false;
        }
        
        public Set<K> get() {
            return set;
        }
    }
    
//    @Test
//    public void testCCS() {
//        String clusterName = "cass_ccs";
//        String keyspaceName = "CacheStatus";
//        
//        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
//        .forCluster(clusterName)
//        .forKeyspace("CacheStatus")
//        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl())
//        .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(clusterName + "_" + keyspaceName)
//            .setPort(PORT)
//            .setSocketTimeout(30000)
//            .setMaxTimeoutWhenExhausted(2000)
//            .setMaxConnsPerHost(1)
//        )
//        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
//        .withHostSupplier(new NetflixDiscoveryHostSupplier(clusterName))
//        .buildKeyspace(ThriftFamilyFactory.getInstance());
//    
//        context.start();
//        Keyspace keyspace = context.getEntity();             
//        
//        ColumnFamily<String, String> cf = ColumnFamily.newColumnFamily("cacheConfig", StringSerializer.get(), StringSerializer.get());
//        try {
//            ToKeySetCallback<String, String> callback = new ToKeySetCallback<String, String>();
//            keyspace.prepareQuery(cf)
//                .getAllRows()
//                .setRepeatLastToken(false)
//                .setRowLimit(5)
//                .setIncludeEmptyRows(true)
//                .executeWithCallback(callback);
//            
//            Set<String> set = callback.get();
//            LOG.info("All columns row count: " + set.size() + " " + set.toString());
//            Assert.assertEquals(16, set.size());
//        } catch (ConnectionException e) {
//            Assert.fail();
//        }
//        
//
//    }
    
    @Test
    public void testGetAllWithTombstonesWithCallback() {
        try {
            ToKeySetCallback callback = new ToKeySetCallback();
            keyspace.prepareQuery(CF_ALL_ROWS_TOMBSTONE)
                .getAllRows()
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .executeWithCallback(callback);
            
            Set<Long> set = callback.get();
            LOG.info("All columns row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(0, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            ToKeySetCallback callback = new ToKeySetCallback();
            keyspace.prepareQuery(CF_ALL_ROWS_TOMBSTONE)
                .getAllRows()
                .setRepeatLastToken(false)
                .setIncludeEmptyRows(true)
                .setRowLimit(5)
                .executeWithCallback(callback);
            
            Set<Long> set = callback.get();
            LOG.info("All columns row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(100, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            ToKeySetCallback callback = new ToKeySetCallback();
            keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .executeWithCallback(callback);
            
            Set<Long> set = callback.get();
            LOG.info("All columns row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(13, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            ToKeySetCallback callback = new ToKeySetCallback();
            keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .withColumnSlice("A")
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .executeWithCallback(callback);
            
            Set<Long> set = callback.get();
            LOG.info("Column='A' Row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(6, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            ToKeySetCallback callback = new ToKeySetCallback();
            keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .withColumnSlice("B")
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .executeWithCallback(callback);
            
            Set<Long> set = callback.get();
            LOG.info("Column='B' Row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(13, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            ToKeySetCallback callback = new ToKeySetCallback();
            keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .withColumnRange(new RangeBuilder().setLimit(1).build())
                .setRowLimit(5)
                .setRepeatLastToken(false)
                .executeWithCallback(callback);
            
            Set<Long> set = callback.get();
            LOG.info("Limit 1 row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(13, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            ToKeySetCallback callback = new ToKeySetCallback();
            keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .withColumnRange(new RangeBuilder().setLimit(0).build())
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .executeWithCallback(callback);
            
            Set<Long> set = callback.get();
            LOG.info("Limit 0 row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(30, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            ToKeySetCallback callback = new ToKeySetCallback();
            keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .executeWithCallback(callback);
            
            Set<Long> set = callback.get();
            LOG.info("All columns row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(13, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        try {
            ToKeySetCallback callback = new ToKeySetCallback();
            keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .setIncludeEmptyRows(true)
                .setRepeatLastToken(false)
                .setRowLimit(5)
                .executeWithCallback(callback);
            
            Set<Long> set = callback.get();
            LOG.info("IncludeEmpty Row count: " + set.size() + " " + set.toString());
            Assert.assertEquals(30, set.size());
        } catch (ConnectionException e) {
            Assert.fail();
        }
    }
    
    
    @Test 
    public void testGetAllWithCallback() {
        try {
            final AtomicLong counter = new AtomicLong();
            
            keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .setRowLimit(3)
                .setRepeatLastToken(false)
                .withColumnRange(new RangeBuilder().setLimit(2).build())
                .executeWithCallback(new RowCallback<Long, String>() {
                    @Override
                    public void success(Rows<Long, String> rows) {
                        for (Row<Long, String> row : rows) {
                            LOG.info("ROW: " + row.getKey() + " " + row.getColumns().size());
                            counter.incrementAndGet();
                        }
                    }

                    @Override
                    public boolean failure(ConnectionException e) {
                        LOG.error(e.getMessage(), e);
                        return false;
                    }
                });
            LOG.info("Read " + counter.get() + " keys");
        } catch (ConnectionException e) {
            Assert.fail();
        }
    }
    
    @Test 
    public void testGetAllWithCallbackThreads() {
        try {
            final AtomicLong counter = new AtomicLong();
            
            keyspace.prepareQuery(CF_ALL_ROWS)
                .getAllRows()
                .setRowLimit(3)
                .setRepeatLastToken(false)
                .setConcurrencyLevel(4)
                .executeWithCallback(new RowCallback<Long, String>() {
                    @Override
                    public void success(Rows<Long, String> rows) {
                        LOG.info(Thread.currentThread().getName());
                        for (Row<Long, String> row : rows) {
                            counter.incrementAndGet();
                        }
                    }

                    @Override
                    public boolean failure(ConnectionException e) {
                        LOG.error(e.getMessage(), e);
                        return false;
                    }
                });
            LOG.info("Read " + counter.get() + " keys");
        } catch (ConnectionException e) {
            LOG.info("Error getting all rows with callback", e);
            Assert.fail();
        }
    }
    
    @Test 
    public void testGetAllWithCallbackThreadsAndCheckpoints() throws Exception {
        try {
            final AtomicLong counter = new AtomicLong();
            
            final CheckpointManager manager = new AstyanaxCheckpointManager(keyspace, CF_CHECKPOINTS.getName(), 123L);
            
            // Read rows in 4 threads
            keyspace.prepareQuery(CF_LOTS_OF_ROWS)
                .getAllRows()
                .setRowLimit(10)
                .setRepeatLastToken(true)
                .setConcurrencyLevel(4)
                .setCheckpointManager(manager)
                .executeWithCallback(new RowCallback<Long, String>() {
                    @Override
                    public void success(Rows<Long, String> rows) {
                        try {
                            LOG.info("Checkpoint: " + manager.getCheckpoints());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        LOG.info(Thread.currentThread().getName());
                        for (Row<Long, String> row : rows) {
                            LOG.info(Thread.currentThread().getName() + " " + row.getKey());
                            counter.incrementAndGet();
                        }
                    }
            
                    @Override
                    public boolean failure(ConnectionException e) {
                        LOG.error(e.getMessage(), e);
                        return false;
                    }
                });
               
            Assert.assertEquals(LOTS_OF_ROWS_COUNT,  counter.get());
            LOG.info("Read " + counter.get() + " keys");
            LOG.info(manager.getCheckpoints().toString());
            
            keyspace.prepareQuery(CF_LOTS_OF_ROWS)
                .getAllRows()
                .setRowLimit(10)
                .setRepeatLastToken(true)
                .setConcurrencyLevel(4)
                .setCheckpointManager(manager)
                .executeWithCallback(new RowCallback<Long, String>() {
                    @Override
                    public void success(Rows<Long, String> rows) {
                        Assert.fail("All rows should have been processed");
                    }
    
                    @Override
                    public boolean failure(ConnectionException e) {
                        LOG.error(e.getMessage(), e);
                        return false;
                    }
                });
        } catch (ConnectionException e) {
            LOG.error("Failed to run test", e);
            Assert.fail();
        }
    }

	@Test
	public void testTokenRangeTest() {
		try {
			OperationResult<Rows<Long, String>> rows = keyspace.prepareQuery(CF_ALL_ROWS)
					.getAllRows()
					.setRowLimit(5)
					.setExceptionCallback(new ExceptionCallback() {
						@Override
						public boolean onException(ConnectionException e) {
							Assert.fail(e.getMessage());
							return true;
						}
					})
					.forTokenRange("9452287970026068429538183539771339207", "37809151880104273718152734159085356828")
					.execute();

			Iterator<Row<Long, String>> itr = rows.getResult().iterator();

			while (itr.hasNext()) {
				Row<Long, String> row = itr.next();
				LOG.info("Row: " + row.getKey() + " count=" + row.getColumns().size());
			}

			Set<Long> set = getKeySet(rows.getResult());
			LOG.info(set.toString());
			// only a subset of the rows should have been returned
			Assert.assertEquals(4,  set.size());
		} catch (ConnectionException e) {
			Assert.fail();
		}
	}
}

package com.netflix.astyanax.recipes;

import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.recipes.locks.StaleLockException;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.test.EmbeddedCassandra;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Ignore for now because of issues with running embedded cassandra from multiple unit tests
 * @author elandau
 *
 */
@Ignore
public class LockRecipeTest {
    private static ColumnFamily<String, String> LOCK_CF_LONG   = 
            ColumnFamily.newColumnFamily("LockCfLong", StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
    
    private static ColumnFamily<String, String> LOCK_CF_STRING = 
            ColumnFamily.newColumnFamily("LockCfString", StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    
    private static final int    TTL                 = 20;
    private static final int    TIMEOUT             = 10;
    private static final String SEEDS               = "localhost:9160";
    private static final long   CASSANDRA_WAIT_TIME = 3000;
    
    private static Keyspace                  keyspace;
    private static AstyanaxContext<Keyspace> keyspaceContext;
    private static EmbeddedCassandra         cassandra;
    
    private static String TEST_CLUSTER_NAME  = "cass_sandbox";
    private static String TEST_KEYSPACE_NAME = "LockUnitTest";
    
    @BeforeClass
    public static void setup() throws Exception {
        System.out.println("TESTING THRIFT KEYSPACE");

        cassandra = new EmbeddedCassandra();
        cassandra.start();
        
        Thread.sleep(CASSANDRA_WAIT_TIME);
        
        createKeyspace();
    }

    @AfterClass
    public static void teardown() {
        if (keyspaceContext != null)
            keyspaceContext.shutdown();
        
        if (cassandra != null)
            cassandra.stop();
    }

    public static void createKeyspace() throws Exception {
        keyspaceContext = new AstyanaxContext.Builder()
                .forCluster(TEST_CLUSTER_NAME)
                .forKeyspace(TEST_KEYSPACE_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME
                                + "_" + TEST_KEYSPACE_NAME)
                                .setSocketTimeout(30000)
                                .setMaxTimeoutWhenExhausted(2000)
                                .setMaxConnsPerHost(10)
                                .setInitConnsPerHost(10)
                                .setSeeds(SEEDS))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        keyspaceContext.start();
        
        keyspace = keyspaceContext.getEntity();
        
        try {
            keyspace.dropKeyspace();
        }
        catch (Exception e) {
            
        }
        
        keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                        .put("replication_factor", "1")
                        .build())
                .put("strategy_class",     "SimpleStrategy")
                .build()
                );
        
        keyspace.createColumnFamily(LOCK_CF_LONG, ImmutableMap.<String, Object>builder()
                .put("default_validation_class", "LongType")
                .put("key_validation_class",     "UTF8Type")
                .put("comparator_type",          "UTF8Type")
                .build());
        
        keyspace.createColumnFamily(LOCK_CF_STRING, ImmutableMap.<String, Object>builder()
                .put("default_validation_class", "UTF8Type")
                .put("key_validation_class",     "UTF8Type")
                .put("comparator_type",          "UTF8Type")
                .build());
        ;
        
        KeyspaceDefinition ki = keyspaceContext.getEntity().describeKeyspace();
        System.out.println("Describe Keyspace: " + ki.getName());

    }
    
    
    @Test
    public void testTtl() throws Exception {
        ColumnPrefixDistributedRowLock<String> lock = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_LONG, "testTtl")
                .withTtl(2)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(1,  TimeUnit.SECONDS);
        
        try {
            lock.acquire();
            Assert.assertEquals(1, lock.readLockColumns().size());
            Thread.sleep(3000);
            Assert.assertEquals(0, lock.readLockColumns().size());
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        finally {
            lock.release();
        }    
        Assert.assertEquals(0, lock.readLockColumns().size());
    }
    
    @Test
    public void testTtlString() throws Exception {
        ColumnPrefixDistributedRowLock<String> lock = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_STRING, "testTtl")
                .withTtl(2)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(1,  TimeUnit.SECONDS);
        
        try {
            lock.acquire();
            Assert.assertEquals(1, lock.readLockColumns().size());
            Thread.sleep(3000);
            Assert.assertEquals(0, lock.readLockColumns().size());
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        finally {
            lock.release();
        }    
        Assert.assertEquals(0, lock.readLockColumns().size());
    }
    
    @Test
    public void testStaleLockWithFail() throws Exception {
        ColumnPrefixDistributedRowLock<String> lock1 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_LONG, "testStaleLock")
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(1, TimeUnit.SECONDS);
        
        ColumnPrefixDistributedRowLock<String> lock2 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_LONG, "testStaleLock")
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(9,  TimeUnit.SECONDS);
        
        try {
            lock1.acquire();
            Thread.sleep(5000);
            try {
                lock2.acquire();
            }
            catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            finally {
                lock2.release();
            }
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        finally {
            lock1.release();
        }
    }
    
    @Test
    public void testStaleLockWithFail_String() throws Exception {
        ColumnPrefixDistributedRowLock<String> lock1 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_STRING, "testStaleLock")
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(1, TimeUnit.SECONDS);
        
        ColumnPrefixDistributedRowLock<String> lock2 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_STRING, "testStaleLock")
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(9,  TimeUnit.SECONDS);
        
        try {
            lock1.acquire();
            Thread.sleep(5000);
            try {
                lock2.acquire();
            }
            catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            finally {
                lock2.release();
            }
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        finally {
            lock1.release();
        }
    }
    
    @Test
    public void testStaleLock() throws Exception {
        ColumnPrefixDistributedRowLock<String> lock1 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_LONG, "testStaleLock")
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(1, TimeUnit.SECONDS);
        
        ColumnPrefixDistributedRowLock<String> lock2 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_LONG, "testStaleLock")
                .failOnStaleLock(true)
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(9, TimeUnit.SECONDS);
        
        try {
            lock1.acquire();
            Thread.sleep(2000);
            try {
                lock2.acquire();
                Assert.fail();
            }
            catch (StaleLockException e) {
            }
            catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            finally {
                lock2.release();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        finally {
            lock1.release();
        }
    }
    @Test
    public void testStaleLock_String() throws Exception {
        ColumnPrefixDistributedRowLock<String> lock1 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_STRING, "testStaleLock")
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(1, TimeUnit.SECONDS);
        
        ColumnPrefixDistributedRowLock<String> lock2 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_STRING, "testStaleLock")
                .failOnStaleLock(true)
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(9, TimeUnit.SECONDS);
        
        try {
            lock1.acquire();
            Thread.sleep(2000);
            try {
                lock2.acquire();
                Assert.fail();
            }
            catch (StaleLockException e) {
            }
            catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            finally {
                lock2.release();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        finally {
            lock1.release();
        }
    }
    
    @Test
    public void testLockAndMutate() throws Exception {
//        String rowKey     = "testLockAndMutate";
//        String dataColumn = "SomeDataColumn";
//        Integer value     = 1;
//        // Write some data
//        try {
//            MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(ConsistencyLevel.CL_ONE);
//            m.withRow(LOCK_CF_LONG, rowKey)
//                .putColumn(dataColumn, value, null);
//            m.execute();
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//            Assert.fail(e.getMessage());
//        }
//        
//        // Take a lock
//        ColumnPrefixDistributedRowLock<String> lock = 
//            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_LONG, rowKey)
//                .expireLockAfter(1, TimeUnit.SECONDS);
//        
//        try {
//            ColumnMap<String> columns = lock
//                .withColumnPrefix("$lock$_")
//                .withLockId("myLockId")
//                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
//                .acquireLockAndReadRow();
//            
//            // Read data and update
//            Assert.assertNotNull(columns);
//            Assert.assertEquals(1, columns.size());
//            
//            value = columns.get(dataColumn).getIntegerValue() + 1;
//            MutationBatch m = keyspace.prepareMutationBatch();
//            m.withRow(LOCK_CF_LONG, rowKey)
//                .putColumn(dataColumn, value, null);
//            
//            // Write data and release the lock
//            lock.releaseWithMutation(m);
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//            Assert.fail(e.getMessage());
//            lock.release();
//        }
//        
//        ColumnList<String> columns = keyspace
//        .prepareQuery(LOCK_CF_LONG)
//            .setConsistencyLevel(ConsistencyLevel.CL_ONE)
//            .getKey(rowKey)
//            .execute()
//                .getResult();
//        Assert.assertEquals(1, columns.size());
//        Assert.assertEquals(value, columns.getIntegerValue(dataColumn, 0));
    }
    
}

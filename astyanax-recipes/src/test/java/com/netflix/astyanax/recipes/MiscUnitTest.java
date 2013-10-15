package com.netflix.astyanax.recipes;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.partitioner.Murmur3Partitioner;
import com.netflix.astyanax.recipes.UUIDStringSupplier;
import com.netflix.astyanax.recipes.functions.ColumnCounterFunction;
import com.netflix.astyanax.recipes.functions.RowCopierFunction;
import com.netflix.astyanax.recipes.functions.RowCounterFunction;
import com.netflix.astyanax.recipes.functions.TraceFunction;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.recipes.locks.StaleLockException;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.recipes.uniqueness.ColumnPrefixUniquenessConstraint;
import com.netflix.astyanax.recipes.uniqueness.DedicatedMultiRowUniquenessConstraint;
import com.netflix.astyanax.recipes.uniqueness.MultiRowUniquenessConstraint;
import com.netflix.astyanax.recipes.uniqueness.NotUniqueException;
import com.netflix.astyanax.recipes.uniqueness.RowUniquenessConstraint;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class MiscUnitTest {
    private static Logger LOG = LoggerFactory.getLogger(MiscUnitTest.class);
    
    /**
     * Constants
     */
    private static final long   CASSANDRA_WAIT_TIME = 3000;
    private static final int    TTL                 = 20;
    
    private static final String TEST_CLUSTER_NAME  = "cass_sandbox";
    private static final String TEST_KEYSPACE_NAME = "AstyanaxUnitTests_MiscRecipes";
    private static final String SEEDS = "localhost:9160";
    
    private static final int    ALL_ROWS_COUNT = 10000;
    
    /**
     * Column Family definitions
     */
    public static ColumnFamily<String, UUID> CF_USER_UNIQUE_UUID = ColumnFamily
            .newColumnFamily(
                    "UserUniqueUUID", 
                    StringSerializer.get(),
                    TimeUUIDSerializer.get());
    
    public static ColumnFamily<String, UUID> CF_EMAIL_UNIQUE_UUID = ColumnFamily
            .newColumnFamily(
                    "EmailUniqueUUID", 
                    StringSerializer.get(),
                    TimeUUIDSerializer.get());
    
    private static ColumnFamily<String, String> LOCK_CF_LONG   = 
            ColumnFamily.newColumnFamily("LockCfLong", StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
    
    private static ColumnFamily<String, String> LOCK_CF_STRING = 
            ColumnFamily.newColumnFamily("LockCfString", StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    
    private static ColumnFamily<String, String> UNIQUE_CF = ColumnFamily
            .newColumnFamily(
                    "UniqueCf", 
                    StringSerializer.get(), 
                    StringSerializer.get());

    public static ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
            .newColumnFamily(
                    "Standard1", 
                    StringSerializer.get(),
                    StringSerializer.get());
    
    public static ColumnFamily<String, String> CF_STANDARD1_COPY = ColumnFamily
            .newColumnFamily(
                    "Standard1_COPY", 
                    StringSerializer.get(),
                    StringSerializer.get());
    
    public static ColumnFamily<Integer, Integer> CF_ALL_ROWS = ColumnFamily
            .newColumnFamily(
                    "AllRowsMiscUnitTest", 
                    IntegerSerializer.get(),
                    IntegerSerializer.get());

    public static ColumnFamily<Integer, Integer> CF_ALL_ROWS_COPY = ColumnFamily
            .newColumnFamily(
                    "AllRowsMiscUnitTestCopy", 
                    IntegerSerializer.get(),
                    IntegerSerializer.get());

    /**
     * Interal
     */
    private static Keyspace                  keyspace;
    private static AstyanaxContext<Keyspace> keyspaceContext;

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
                                .setSeeds(SEEDS))
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
        

        keyspace.createColumnFamily(CF_USER_UNIQUE_UUID,  null);
        keyspace.createColumnFamily(CF_EMAIL_UNIQUE_UUID, null);
        keyspace.createColumnFamily(CF_ALL_ROWS, null);
        
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
        
        keyspace.createColumnFamily(CF_STANDARD1, ImmutableMap.<String, Object>builder()
                .put("column_metadata", ImmutableMap.<String, Object>builder()
                        .put("Index1", ImmutableMap.<String, Object>builder()
                                .put("validation_class", "UTF8Type")
                                .put("index_name",       "Index1")
                                .put("index_type",       "KEYS")
                                .build())
                        .put("Index2", ImmutableMap.<String, Object>builder()
                                .put("validation_class", "UTF8Type")
                                .put("index_name",       "Index2")
                                .put("index_type",       "KEYS")
                                .build())
                         .build())
                     .build());
        
        keyspace.createColumnFamily(UNIQUE_CF, null);
        keyspace.createColumnFamily(CF_STANDARD1_COPY, null);
        
        KeyspaceDefinition ki = keyspaceContext.getEntity().describeKeyspace();
        System.out.println("Describe Keyspace: " + ki.getName());
        
        try {
            //
            // CF_Super :
            // 'A' :
            // 'a' :
            // 1 : 'Aa1',
            // 2 : 'Aa2',
            // 'b' :
            // ...
            // 'z' :
            // ...
            // 'B' :
            // ...
            //
            // CF_Standard :
            // 'A' :
            // 'a' : 1,
            // 'b' : 2,
            // ...
            // 'z' : 26,
            // 'B' :
            // ...
            //

            MutationBatch m;
            OperationResult<Void> result;
            m = keyspace.prepareMutationBatch();

            for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
                String rowKey = Character.toString(keyName);
                ColumnListMutation<String> cfmStandard = m.withRow(
                        CF_STANDARD1, rowKey);
                for (char cName = 'a'; cName <= 'z'; cName++) {
                    cfmStandard.putColumn(Character.toString(cName),
                            (int) (cName - 'a') + 1, null);
                }
                cfmStandard
                        .putColumn("Index1", (int) (keyName - 'A') + 1, null);
                cfmStandard.putColumn("Index2", 42, null);
                m.execute();
            }

            m.withRow(CF_STANDARD1, "Prefixes").putColumn("Prefix1_a", 1, null)
                    .putColumn("Prefix1_b", 2, null)
                    .putColumn("prefix2_a", 3, null);

            result = m.execute();

            m.execute();
            
            m = keyspace.prepareMutationBatch();
            for (int i = 0; i < ALL_ROWS_COUNT; i++) {
                m.withRow(CF_ALL_ROWS,  i).putColumn(0,  true);
                if (m.getRowCount() == 50) {
                    m.execute();
                }
            }
            m.execute();

        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testMultiRowUniqueness() {
        DedicatedMultiRowUniquenessConstraint<UUID> constraint = new DedicatedMultiRowUniquenessConstraint<UUID>
                  (keyspace, TimeUUIDUtils.getUniqueTimeUUIDinMicros())
                  .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                  .withRow(CF_USER_UNIQUE_UUID, "user1")
                  .withRow(CF_EMAIL_UNIQUE_UUID, "user1@domain.com");
        
        DedicatedMultiRowUniquenessConstraint<UUID> constraint2 = new DedicatedMultiRowUniquenessConstraint<UUID>
                  (keyspace, TimeUUIDUtils.getUniqueTimeUUIDinMicros())
                  .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                  .withRow(CF_USER_UNIQUE_UUID, "user1")
                  .withRow(CF_EMAIL_UNIQUE_UUID, "user1@domain.com");
        
        try {
            Column<UUID> c = constraint.getUniqueColumn();
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info(e.getMessage());
        }
        
        try {
            constraint.acquire();
            
            Column<UUID> c = constraint.getUniqueColumn();
            LOG.info("Unique column is " + c.getName());
            
            try {
                constraint2.acquire();
                Assert.fail("Should already be acquired");
            }
            catch (NotUniqueException e) {
                
            }
            catch (Exception e) {
                e.printStackTrace();
                Assert.fail();
            }
            finally {
                try {
                    constraint2.release();
                }
                catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        finally {
            try {
                constraint.release();
            }
            catch (Exception e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
        
        try {
            constraint2.acquire();
            Column<UUID> c = constraint.getUniqueColumn();
            LOG.info("Unique column is " + c.getName());
        }
        catch (NotUniqueException e) {
            Assert.fail("Should already be unique");
        }
        catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        finally {
            try {
                constraint2.release();
            }
            catch (Exception e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
        
    }

//    @Test
//    public void testAllRowsReaderConcurrency() throws Exception {
//        final AtomicLong counter = new AtomicLong(0);
//        
//        boolean result = new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
//                .withConcurrencyLevel(4)
//                .forEachRow(new Function<Row<String, String>, Boolean>() {
//                    @Override
//                    public Boolean apply(@Nullable Row<String, String> row) {
//                        counter.incrementAndGet();
//                        LOG.info("Got a row: " + row.getKey().toString());
//                        return true;
//                    }
//                })
//                .build()
//                .call();
//        
//        Assert.assertTrue(result);
//        Assert.assertEquals(28, counter.get());
//    }

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
    public void testMultiLock() {
        MultiRowUniquenessConstraint unique = new MultiRowUniquenessConstraint(keyspace)
            .withConsistencyLevel(ConsistencyLevel.CL_ONE)
            .withTtl(60)
            .withLockId("abc")
            .withColumnPrefix("prefix_")
            .withRow(UNIQUE_CF, "testMultiLock_A")
            .withRow(UNIQUE_CF, "testMultiLock_B");
        
        ColumnPrefixUniquenessConstraint<String> singleUnique 
            = new ColumnPrefixUniquenessConstraint<String>(keyspace, UNIQUE_CF, "testMultiLock_A")
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .withPrefix("prefix_");
        try {
            unique.acquire();
            String uniqueColumn = singleUnique.readUniqueColumn();
            Assert.assertEquals("abc", uniqueColumn);
            LOG.info("UniqueColumn: " + uniqueColumn);
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        
        MultiRowUniquenessConstraint unique2 = new MultiRowUniquenessConstraint(keyspace)
            .withTtl(60)
            .withConsistencyLevel(ConsistencyLevel.CL_ONE)
            .withColumnPrefix("prefix_")
            .withRow(UNIQUE_CF, "testMultiLock_B");
        try {
            unique2.acquire();
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info(e.getMessage());
        }
        
        try {
            Assert.assertEquals("abc", singleUnique.readUniqueColumn());
            unique.release();
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        
        try {
            unique2.acquire();
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
        
        try {
            unique2.release();
        } catch (Exception e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
    }
    
    @Test
    public void testRowUniquenessConstraint() throws Exception {
        RowUniquenessConstraint<String, String> unique = new RowUniquenessConstraint<String, String>
                (keyspace, UNIQUE_CF, "testRowUniquenessConstraint", UUIDStringSupplier.getInstance())
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                ;
        RowUniquenessConstraint<String, String> unique2 = new RowUniquenessConstraint<String, String>
                (keyspace, UNIQUE_CF, "testRowUniquenessConstraint", UUIDStringSupplier.getInstance())
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                ;
        
        try {
            unique.withData("abc").acquire();
            try {
                unique2.acquire();
                Assert.fail();
            }
            catch (Exception e) {
                LOG.info(e.getMessage());
            }
            
            String data = unique.readDataAsString();
            Assert.assertNotNull(data);
        }
        catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
            LOG.error(e.getMessage());
        }
        finally {
            unique.release();
        }
        
        try {
            String data = unique.readDataAsString();
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info("", e);
        }
    }

    @Test
    public void testPrefixUniquenessConstraint() throws Exception {
        ColumnPrefixUniquenessConstraint<String> unique = new ColumnPrefixUniquenessConstraint<String>(
                keyspace, UNIQUE_CF, "testPrefixUniquenessConstraint")
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                ;
        ColumnPrefixUniquenessConstraint<String> unique2 = new ColumnPrefixUniquenessConstraint<String>(
                keyspace, UNIQUE_CF, "testPrefixUniquenessConstraint")
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                ;
        
        try {
            unique.acquire();
            String column = unique.readUniqueColumn();
            LOG.info("Unique Column: " + column);
            
            try {
                unique2.acquire();
                Assert.fail();
            }
            catch (Exception e) {
                
            }
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
            LOG.error(e.getMessage());
        }
        finally {
            unique.release();
        }

        try {
            String column = unique.readUniqueColumn();
            LOG.info(column);
            Assert.fail();
        }
        catch (Exception e) {
            
        }
    }

    @Test
    public void testPrefixUniquenessConstraintWithColumn() throws Exception {
        ColumnPrefixUniquenessConstraint<String> unique = new ColumnPrefixUniquenessConstraint<String>(
                keyspace, UNIQUE_CF, "testPrefixUniquenessConstraintWithColumn")
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .withUniqueId("abc");
        ColumnPrefixUniquenessConstraint<String> unique2 = new ColumnPrefixUniquenessConstraint<String>(
                keyspace, UNIQUE_CF, "testPrefixUniquenessConstraintWithColumn")
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .withUniqueId("def");
        
        try {
            unique.acquire();
            
            String column = unique.readUniqueColumn();
            LOG.info("Unique Column: " + column);
            Assert.assertEquals("abc", column);
            
            try {
                unique2.acquire();
                Assert.fail();
            }
            catch (Exception e) {
                
            }
            
            column = unique.readUniqueColumn();
            LOG.info("Unique Column: " + column);
            Assert.assertEquals("abc", column);
            
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
            LOG.error(e.getMessage());
        }
        finally {
            unique.release();
        }
    }
    
    @Test 
    public void testAcquireAndMutate() throws Exception {
        final String row        = "testAcquireAndMutate";
        final String dataColumn = "data";
        final String value      = "test";
        
        ColumnPrefixUniquenessConstraint<String> unique = new ColumnPrefixUniquenessConstraint<String>(
                keyspace, UNIQUE_CF, row)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .withUniqueId("def");
        
        try {
            unique.acquireAndApplyMutation(new Function<MutationBatch, Boolean>() {
                @Override
                public Boolean apply(MutationBatch m) {
                    m.withRow(UNIQUE_CF, row)
                        .putColumn(dataColumn, value, null);
                    return true;
                }
            });
            String column = unique.readUniqueColumn();
            Assert.assertNotNull(column);
        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error("", e);
            Assert.fail();
        }
        finally {
        }
        
        ColumnList<String> columns = keyspace.prepareQuery(UNIQUE_CF).getKey(row).execute().getResult();
        Assert.assertEquals(2, columns.size());
        Assert.assertEquals(value, columns.getStringValue(dataColumn, null));
        
        unique.release();
        
        columns = keyspace.prepareQuery(UNIQUE_CF).getKey(row).execute().getResult();
        Assert.assertEquals(1, columns.size());
        Assert.assertEquals(value, columns.getStringValue(dataColumn, null));
    }

//    @Test
//    public void testAllRowsReader() throws Exception {
//        final AtomicLong counter = new AtomicLong(0);
//        
//        boolean result = new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
//                .forEachRow(new Function<Row<String, String>, Boolean>() {
//                    @Override
//                    public Boolean apply(@Nullable Row<String, String> row) {
//                        counter.incrementAndGet();
//                        LOG.info("Got a row: " + row.getKey().toString());
//                        return true;
//                    }
//                })
//                .build()
//                .call();
//        
//        Assert.assertTrue(result);
//        Assert.assertEquals(28, counter.get());
//    }
    
    @Test
    public void testAllRowsReader() throws Exception {
        final AtomicLong counter = new AtomicLong(0);
        
        AllRowsReader<String, String> reader = new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
                .withPageSize(3)
                .withConcurrencyLevel(2)
//                .withPartitioner(new Murmur3Partitioner())
                .forEachRow(new Function<Row<String, String>, Boolean>() {
                    @Override
                    public Boolean apply(Row<String, String> row) {
                        counter.incrementAndGet();
                        LOG.info("Got a row: " + row.getKey().toString());
                        return true;
                    }
                })
                .build();
        
        try {
            boolean result = reader.call();
            Assert.assertEquals(counter.get(), 27);
            Assert.assertTrue(result);
        }
        catch (Exception e) {
            LOG.info(e.getMessage(), e);
            Assert.fail(e.getMessage());
        }
        
    }
    
    @Test
    public void testAllRowsReaderCopier() throws Exception {
        final ColumnCounterFunction columnCounter = new ColumnCounterFunction();
        final RowCounterFunction    rowCounter    = new RowCounterFunction();
                
        new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
                .withPageSize(3)
                .withConcurrencyLevel(2)
                .forEachRow(columnCounter)
                .build()
                .call();
        
        LOG.info("Column count = " + columnCounter.getCount());
        
        new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
                .withPageSize(3)
                .withConcurrencyLevel(2)
                .forEachRow(rowCounter)
                .build()
                .call();
        
        LOG.info("Row count = " + rowCounter.getCount());
        
        new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
                .withPageSize(3)
                .withConcurrencyLevel(2)
                .forEachRow(RowCopierFunction.builder(keyspace, CF_STANDARD1_COPY).build())
                .build()
                .call();

        rowCounter.reset();
        new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1_COPY)
            .withPageSize(3)
            .withConcurrencyLevel(2)
            .forEachRow(rowCounter)
            .build()
            .call();

        LOG.info("Copied row count = " + rowCounter.getCount());
        
        LOG.info("CF_STANDARD1");
        new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
            .withPageSize(3)
            .withConcurrencyLevel(2)
            .forEachRow(TraceFunction.builder(CF_STANDARD1_COPY).build())
            .build()
            .call();

        LOG.info("CF_STANDARD1_COPY");
        new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1_COPY)
            .withPageSize(3)
            .withConcurrencyLevel(2)
            .forEachRow(TraceFunction.builder(CF_STANDARD1_COPY).build())
            .build()
            .call();
    }
    
    @Test
    public void testAllRowsReaderConcurrency12() throws Exception {
        final AtomicLong counter = new AtomicLong(0);
        
        final Map<Long, AtomicLong> threadIds = Maps.newHashMap();
        
        AllRowsReader<Integer, Integer> reader = new AllRowsReader.Builder<Integer, Integer>(keyspace, CF_ALL_ROWS)
                .withPageSize(100)
                .withConcurrencyLevel(12)
                .withColumnSlice(0)
                .forEachRow(new Function<Row<Integer, Integer>, Boolean>() {
                    @Override
                    public synchronized Boolean apply(Row<Integer, Integer> row) {
                        long threadId = Thread.currentThread().getId();
                        AtomicLong threadCounter = threadIds.get(threadId);
                        if (threadCounter == null) {
                            threadCounter = new AtomicLong(0);
                            threadIds.put(threadId, threadCounter);
                        }
                        threadCounter.incrementAndGet();
                        counter.incrementAndGet();
                        return true;
                    }
                })
                .build();
        
        try {
            Stopwatch sw = new Stopwatch().start();
            boolean result = reader.call();
            long runtimeMillis = sw.stop().elapsedMillis();
            
            LOG.info("Count = " + counter.get() + " runtime=" + runtimeMillis);
            LOG.info("ThreadIds (" + threadIds.size() + ") " + threadIds);
            Assert.assertEquals(threadIds.size(), 12);
            Assert.assertEquals(counter.get(), ALL_ROWS_COUNT);
            Assert.assertTrue(result);
        }
        catch (Exception e) {
            LOG.info(e.getMessage(), e);
            Assert.fail(e.getMessage());
        }
        
    }
    
    
    @Test
    public void testAllRowsReaderWithCancel() throws Exception {
        final AtomicLong counter = new AtomicLong(0);
        
        AllRowsReader<String, String> reader = new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
                .withPageSize(3)
                .withConcurrencyLevel(2)
                .forEachRow(new Function<Row<String, String>, Boolean>() {
                    @Override
                    public Boolean apply(Row<String, String> row) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        counter.incrementAndGet();
                        LOG.info("Got a row: " + row.getKey().toString());
                        return true;
                    }
                })
                .build();
        
        
        Future<Boolean> future = Executors.newSingleThreadExecutor().submit(reader);        
        
        Thread.sleep(1000);
        
        reader.cancel();
        
        try {
            boolean result = future.get();
            Assert.assertEquals(false, result);
        }
        catch (Exception e) {
            LOG.info("Failed to execute", e);
        }
        LOG.info("Before: " + counter.get());
        Assert.assertNotSame(28, counter.get());
        Thread.sleep(2000);
        LOG.info("After: " + counter.get());
        Assert.assertNotSame(28, counter.get());
    }


    @Test
    public void testAllRowsReaderWithException() throws Exception {
        AllRowsReader<String, String> reader = new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
                .withPageSize(3)
                .withConcurrencyLevel(2)
                .forEachRow(new Function<Row<String, String>, Boolean>() {
                    @Override
                    public Boolean apply(Row<String, String> row) {
                        throw new RuntimeException("Very bad");
                    }
                })
                .build();
        
        
        Future<Boolean> future = Executors.newSingleThreadExecutor().submit(reader);        
        
        try {
            boolean result = future.get();
            Assert.fail();
        }
        catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Very bad"));
            LOG.info("Failed to execute", e);
        }
    }
}

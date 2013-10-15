package com.netflix.astyanax.entitystore;

import java.util.Collection;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;

public class CompositeEntityManagerTest {
    private static Logger LOG = LoggerFactory.getLogger(CompositeEntityManagerTest.class);
    
    private static Keyspace                  keyspace;
    private static AstyanaxContext<Keyspace> keyspaceContext;

    private static String TEST_CLUSTER_NAME  = "junit_cass_sandbox";
    private static String TEST_KEYSPACE_NAME = "CompositeEntityManagerTest";
    private static final String SEEDS        = "localhost:9160";

    @Entity
    public static class TestEntity {
        public TestEntity() {
        }
        
        public TestEntity(String rowKey, String part1, Long part2, Long value) {
            super();
            this.part1 = part1;
            this.part2 = part2;
            this.value = value;
            this.rowKey = rowKey;
        }
        
        @Id     String rowKey;      // This will be the row key
        @Column String part1;       // This will be the first part of the composite
        @Column Long   part2;       // This will be the second part of the composite
        @Column Long   value;       // This will be the value of the composite
        
        @Override
        public String toString() {
            return "TestEntityChild ["
                    +   "key="   + rowKey 
                    + ", part1=" + part1 
                    + ", part2=" + part2
                    + ", value=" + value + "]";
        }
    }

    @BeforeClass
    public static void setup() throws Exception {

        SingletonEmbeddedCassandra.getInstance();

        Thread.sleep(1000 * 3);

        createKeyspace();

        Thread.sleep(1000 * 3);
        
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (keyspaceContext != null)
            keyspaceContext.shutdown();

        Thread.sleep(1000 * 10);
    }

    private static CompositeEntityManager<TestEntity, String> manager;

    private static void createKeyspace() throws Exception {
        keyspaceContext = new AstyanaxContext.Builder()
        .forCluster(TEST_CLUSTER_NAME)
        .forKeyspace(TEST_KEYSPACE_NAME)
        .withAstyanaxConfiguration(
                new AstyanaxConfigurationImpl()
                .setCqlVersion("3.0.0")
                .setTargetCassandraVersion("1.2")
                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE))
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

        keyspace = keyspaceContext.getClient();

        try {
            keyspace.dropKeyspace();
        }
        catch (Exception e) {
            LOG.info(e.getMessage(), e);
        }

        keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                        .put("replication_factor", "1")
                        .build())
                        .put("strategy_class",     "SimpleStrategy")
                        .build()
                );
        
        manager = CompositeEntityManager.<TestEntity, String>builder()
                    .withKeyspace(keyspace)
                    .withColumnFamily("testentity")
                    .withEntityType(TestEntity.class)
                    .withVerboseTracing(true)
                    .build();
        
        manager.createStorage(null);

        List<TestEntity> children = Lists.newArrayList();
        
        for (long i = 0; i < 10; i++) {
            children.add(new TestEntity("A", "a", i, i*i));
            children.add(new TestEntity("A", "b", i, i*i));
            children.add(new TestEntity("B", "a", i, i*i));
            children.add(new TestEntity("B", "b", i, i*i));
        }
        
        manager.put(children);
        
        // Read back all rows and log
        logResultSet(manager.getAll(), "ALL: ");
    }

    @Test
    public void test() throws Exception {
        List<TestEntity> cqlEntities;
        Collection<TestEntity> entitiesNative;

        // Simple row query
        entitiesNative = manager.createNativeQuery()
                .whereId().in("A")
                .getResultSet();
        Assert.assertEquals(20, entitiesNative.size());
        LOG.info("NATIVE: " + entitiesNative);
        
        // Multi row query
        cqlEntities = manager.find("SELECT * from TestEntity WHERE KEY IN ('A', 'B')");
        Assert.assertEquals(40,  cqlEntities.size());
        
        entitiesNative = manager.createNativeQuery()
                .whereId().in("A", "B")
                .getResultSet();
        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(40, entitiesNative.size());
        
        // Simple prefix
        entitiesNative = manager.createNativeQuery()
                .whereId().equal("A")
                .whereColumn("part1").equal("a")
                .getResultSet();
        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(10, entitiesNative.size());
        
        cqlEntities = manager.find("SELECT * from TestEntity WHERE KEY = 'A' AND column1='b' AND column2>=5 AND column2<8");
        Assert.assertEquals(3,  cqlEntities.size());
        LOG.info(cqlEntities.toString());
        
        manager.remove(new TestEntity("A", "b", 5L, null));
        cqlEntities = manager.find("SELECT * from TestEntity WHERE KEY = 'A' AND column1='b' AND column2>=5 AND column2<8");
        Assert.assertEquals(2,  cqlEntities.size());
        LOG.info(cqlEntities.toString());
        
        manager.delete("A");
        cqlEntities = manager.find("SELECT * from TestEntity WHERE KEY = 'A' AND column1='b' AND column2>=5 AND column2<8");
        Assert.assertEquals(0,  cqlEntities.size());
    }
    
    @Test
    public void testQuery() throws Exception {
        Collection<TestEntity> entitiesNative;

        entitiesNative = manager.createNativeQuery()
                .whereId().in("B")
                .whereColumn("part1").equal("b")
                .whereColumn("part2").greaterThanEqual(5L)
                .whereColumn("part2").lessThan(8L)
                .getResultSet();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(3, entitiesNative.size());
    }
    
//  ... Not sure this use case makes sense since cassandra will end up returning
//  columns with part2 greater than 8 but less than b
//    @Test
//    public void testQueryComplexRange() throws Exception {
//        Collection<TestEntity> entitiesNative;
//
//        entitiesNative = manager.createNativeQuery()
//                .whereId().in("B")
//                .whereColumn("part1").lessThan("b")
//                .whereColumn("part2").lessThan(8L)
//                .getResultSet();
//
//        LOG.info("NATIVE: " + entitiesNative.toString());
//        logResultSet(manager.getAll(), "COMPLEX RANGE: ");
//        Assert.assertEquals(2, entitiesNative.size());
//    }
    
    @Test
    public void testBadFieldName() throws Exception {

        try {
            manager.createNativeQuery()
                    .whereId().in("A")
                    .whereColumn("badfield").equal("b")
                    .getResultSet();
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info(e.getMessage(), e);
        }
    }
    
    private static void logResultSet(List<TestEntity> result, String prefix) {
        // Read back all rows and log
        List<TestEntity> all = manager.getAll();
        for (TestEntity entity : all) {
            LOG.info(prefix + entity.toString());
        }
    }
}

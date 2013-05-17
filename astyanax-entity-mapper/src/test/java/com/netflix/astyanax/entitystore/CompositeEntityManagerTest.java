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
        
        public TestEntity(String rowKey, List<TestEntityChild> children) {
            super();
            this.rowKey   = rowKey;
            this.children = children;
        }

        @Id     
        private String rowKey;      // This will be the row key
        
        @OneToMany
        private List<TestEntityChild> children;

        public TestEntity addChild(TestEntityChild entity) {
            if (children == null)
                children = Lists.newArrayList();
            children.add(entity);
            return this;
        }
        
        public String getRowKey() {
            return rowKey;
        }

        public TestEntity setRowKey(String rowKey) {
            this.rowKey = rowKey;
            return this;
        }

        public List<TestEntityChild> getChildren() {
            return children;
        }

        public TestEntity setChildren(List<TestEntityChild> children) {
            this.children = children;
            return this;
        }

        @Override
        public String toString() {
            return "TestEntity [rowKey=" + rowKey + ", children=" + children
                    + "]";
        }
        
    }
    
    @Entity
    public static class TestEntityChild {
        public TestEntityChild() {
        }
        
        public TestEntityChild(String part1, Long part2, Long value) {
            super();
            this.part1 = part1;
            this.part2 = part2;
            this.value = value;
        }

        @Column String part1;       // This will be the first part of the composite
        @Column Long   part2;       // This will be the second part of the composite
        @Column Long   value;       // This will be the value of the composite
        
        @Override
        public String toString() {
            return "TestEntityChild [part1=" + part1 + ", part2=" + part2
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
                    .withColumnFamily("TestEntity")
                    .withEntityType(TestEntity.class)
                    .build();
        
        manager.createStorage(null);

        List<TestEntityChild> children = Lists.newArrayList();
        
        for (long i = 0; i < 10; i++) {
            children.add(new TestEntityChild("a", i, i*i));
            children.add(new TestEntityChild("b", i, i*i));
        }
        
        manager.put(new TestEntity("A", children));
        manager.put(new TestEntity("B", children));
    }

    @Test
    public void test() throws Exception {
        List<TestEntity> cqlEntities;
        Collection<TestEntity> entitiesNative;

        // Simple row query
        TestEntity entity = manager.get("A");
        LOG.info("Result : " + entity);

        entitiesNative = manager.createNativeQuery()
                .whereId().in("A")
                .getResultSet();
        Assert.assertEquals(entitiesNative.size(), 1);
        LOG.info("NATIVE: " + entitiesNative);
        
        // Multi row query
        cqlEntities = manager.find("SELECT * from TestEntity WHERE KEY IN ('A', 'B')");
        Assert.assertEquals(2,  cqlEntities.size());
        
        entitiesNative = manager.createNativeQuery()
                .whereId().in("A", "B")
                .getResultSet();
        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(entitiesNative.size(), 2);
        
        // Simple prefix
        entitiesNative = manager.createNativeQuery()
                .whereId().equal("A")
                .whereColumn("part1").equal("a")
                .getResultSet();
        LOG.info("NATIVE: " + entitiesNative);
        Assert.assertEquals(entitiesNative.size(), 1);
        
        cqlEntities = manager.find("SELECT * from TestEntity WHERE KEY = 'A' AND column1='b' AND column2>=5 AND column2<8");
        Assert.assertEquals(1,  cqlEntities.size());
        Assert.assertEquals(3,  Iterables.getFirst(cqlEntities, null).children.size());
        LOG.info(cqlEntities.toString());
        
        manager.remove(new TestEntity().setRowKey("A").addChild(new TestEntityChild("B", 5L, null)));
        cqlEntities = manager.find("SELECT * from TestEntity WHERE KEY = 'A' AND column1='b' AND column2>=5 AND column2<8");
        Assert.assertEquals(1,  cqlEntities.size());
        Assert.assertEquals(3,  Iterables.getFirst(cqlEntities, null).children.size());
        LOG.info(cqlEntities.toString());
        
        manager.remove(new TestEntity().setRowKey("A"));
        cqlEntities = manager.find("SELECT * from TestEntity WHERE KEY = 'A' AND column1='b' AND column2>=5 AND column2<8");
        Assert.assertEquals(0,  cqlEntities.size());
    }
    
    @Test
    public void testQuery() throws Exception {
        Collection<TestEntity> entitiesNative;

        entitiesNative = manager.createNativeQuery()
                .whereId().in("A")
                .whereColumn("part1").equal("b")
                .whereColumn("part2").greaterThanEqual(5L)
                .whereColumn("part2").lessThan(8L)
                .getResultSet();

        LOG.info("NATIVE: " + entitiesNative.toString());
        Assert.assertEquals(entitiesNative.size(), 1);
    }
    
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
            e.printStackTrace();
        }
    }
}

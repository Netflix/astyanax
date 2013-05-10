package com.netflix.astyanax.entitystore;

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

import com.google.common.collect.ImmutableList;
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

        @Id     String part1;       // This will be the first part of the composite
        @Id     Long   part2;       // This will be the second part of the composite
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

        keyspace = keyspaceContext.getEntity();

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
    }

    @Test
    public void test() {
        CompositeEntityManager<TestEntity, String> manager = 
                CompositeEntityManager.<TestEntity, String>builder()
                    .withKeyspace(keyspace)
                    .withColumnFamily("TestEntity")
                    .withEntityType(TestEntity.class)
                    .build();
        
        manager.createStorage(null);

        List<TestEntityChild> children = Lists.newArrayList();
        
        for (long i = 0; i < 10; i++) {
            children.add(new TestEntityChild("B", i, i*i));
        }
        
        manager.put(new TestEntity("A", children));
        manager.put(new TestEntity("B", children));
        
        TestEntity entity = manager.get("A");
        LOG.info("Result : " + entity);

        List<TestEntity> entities = manager.find("SELECT * from TestEntity WHERE KEY IN ('A', 'B')");
        Assert.assertEquals(2,  entities.size());
        
        entities = manager.find("SELECT * from TestEntity WHERE KEY = 'A' AND column1='B' AND column2>=5 AND column2<8");
        Assert.assertEquals(1,  entities.size());
        Assert.assertEquals(3,  Iterables.getFirst(entities, null).children.size());
        LOG.info(entities.toString());
        
        manager.remove(new TestEntity().setRowKey("A").addChild(new TestEntityChild("B", 5L, null)));
        entities = manager.find("SELECT * from TestEntity WHERE KEY = 'A' AND column1='B' AND column2>=5 AND column2<8");
        Assert.assertEquals(1,  entities.size());
        Assert.assertEquals(2,  Iterables.getFirst(entities, null).children.size());
        LOG.info(entities.toString());
        
        manager.remove(new TestEntity().setRowKey("A"));
        entities = manager.find("SELECT * from TestEntity WHERE KEY = 'A' AND column1='B' AND column2>=5 AND column2<8");
        Assert.assertEquals(0,  entities.size());
//        manager.find
    }
}

package com.netflix.astyanax.thrift;

import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;

public class ThriftClusterImplTest {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftClusterImplTest.class);
    
    private static final String SEEDS = "localhost:9160";
    private static final long   CASSANDRA_WAIT_TIME = 3000;
    private static String TEST_CLUSTER_NAME  = "cass_sandbox";
    private static String TEST_KEYSPACE_NAME = "AstyanaxUnitTests";
    
    private static AstyanaxContext<Cluster> context;
    private static Cluster cluster;
    
    @BeforeClass
    public static void setup() throws Exception {
        System.out.println("TESTING THRIFT KEYSPACE");

        SingletonEmbeddedCassandra.getInstance();
        
        Thread.sleep(CASSANDRA_WAIT_TIME);
        
        context = new AstyanaxContext.Builder()
                .forCluster(TEST_CLUSTER_NAME)
                .forKeyspace(TEST_KEYSPACE_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
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
                .buildCluster(ThriftFamilyFactory.getInstance());
        
        context.start();
        
        cluster = context.getClient();

    }

    @AfterClass
    public static void teardown() throws Exception {
        if (context != null)
            context.shutdown();
        
        Thread.sleep(CASSANDRA_WAIT_TIME);
    }

    @Test
    public void test() throws Exception {
        String keyspaceName = "ClusterTest";
        
        Properties props = new Properties();
        props.put("name",                                keyspaceName);
        props.put("strategy_class",                      "SimpleStrategy");
        props.put("strategy_options.replication_factor", "1");
        
        cluster.createKeyspace(props);
        
        Properties prop1 = cluster.getKeyspaceProperties(keyspaceName);
        System.out.println(prop1);
        Assert.assertTrue(prop1.containsKey("name"));
        Assert.assertTrue(prop1.containsKey("strategy_class"));
        
        Properties prop2 = cluster.getAllKeyspaceProperties();
        System.out.println(prop2);
        Assert.assertTrue(prop2.containsKey("ClusterTest.name"));
        Assert.assertTrue(prop2.containsKey("ClusterTest.strategy_class"));
        
        Properties cfProps = new Properties();
        cfProps.put("keyspace",   keyspaceName);
        cfProps.put("name",       "cf1");
        cfProps.put("compression_options.sstable_compression", "");
        
        cluster.createColumnFamily(cfProps);
        
        Properties cfProps1 = cluster.getKeyspaceProperties(keyspaceName);
        KeyspaceDefinition ksdef = cluster.describeKeyspace(keyspaceName);
        ColumnFamilyDefinition cfdef = ksdef.getColumnFamily("cf1");
        LOG.info(cfProps1.toString());
        
        LOG.info(cfdef.getProperties().toString());
        Assert.assertEquals(cfProps1.get("cf_defs.cf1.comparator_type"), "org.apache.cassandra.db.marshal.BytesType");
        
    }
}

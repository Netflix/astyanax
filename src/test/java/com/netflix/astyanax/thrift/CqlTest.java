package com.netflix.astyanax.thrift;

import java.util.UUID;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
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
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class CqlTest {

    private static Logger LOG = LoggerFactory.getLogger(CqlTest.class);

    private static Keyspace                  keyspace;
    private static AstyanaxContext<Keyspace> keyspaceContext;

    private static String TEST_CLUSTER_NAME  = "cass_sandbox";
    private static String TEST_KEYSPACE_NAME = "CqlTest";

    private static final String SEEDS = "localhost:9160";

    private static final long   CASSANDRA_WAIT_TIME = 1000;
    private static final int    TTL                 = 20;
    private static final int    TIMEOUT             = 10;
    
    static ColumnFamily<String, String> CQL3_CF = ColumnFamily.newColumnFamily(
            "Cql3CF", 
            StringSerializer.get(), 
            StringSerializer.get());
    
    @BeforeClass
    public static void setup() throws Exception {
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
                                .setDiscoveryDelayInSeconds(60000)
                                .setCqlVersion("3.0.0"))
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
            Thread.sleep(CASSANDRA_WAIT_TIME);
        }
        catch (Exception e) {
            LOG.info("Error dropping keyspace " + e.getMessage());
        }
        
        keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                        .put("replication_factor", "1")
                        .build())
                .put("strategy_class",     "SimpleStrategy")
                .build()
                );
        
        Thread.sleep(CASSANDRA_WAIT_TIME);
        
        OperationResult<CqlResult<String, String>> result;

        result = keyspace.prepareQuery(CQL3_CF)
            .withCql("CREATE TABLE employees (empID int, deptID int, first_name varchar, last_name varchar, PRIMARY KEY (empID, deptID));")
            .execute();
        
        Thread.sleep(CASSANDRA_WAIT_TIME);
        
        KeyspaceDefinition ki = keyspaceContext.getEntity().describeKeyspace();
        System.out.println("Describe Keyspace: " + ki.getName());
        
    }
    
    @Test
    public void testCompoundKey() throws Exception {
        OperationResult<CqlResult<String, String>> result;
        result = keyspace
                .prepareQuery(CQL3_CF)
                .withCql("INSERT INTO employees (empID, deptID, first_name, last_name) VALUES ('111', '222', 'eran', 'landau');")
                .execute();
        
        result = keyspace
                .prepareQuery(CQL3_CF)
                .withCql("INSERT INTO employees (empID, deptID, first_name, last_name) VALUES ('111', '233', 'netta', 'landau');")
                .execute();
        
        result = keyspace
                .prepareQuery(CQL3_CF)
                .withCql("SELECT * FROM employees WHERE empId='111';")
                .execute();

        for (Row<String, String> row : result.getResult().getRows()) {
            LOG.info("CQL Key: " + row.getKey());

            ColumnList<String> columns = row.getColumns();
            
            LOG.info("   empid      : " + columns.getIntegerValue("empid",      null));
            LOG.info("   deptid     : " + columns.getIntegerValue("deptid",     null));
            LOG.info("   first_name : " + columns.getStringValue ("first_name", null));
            LOG.info("   last_name  : " + columns.getStringValue ("last_name",  null));
        }   
    }

}

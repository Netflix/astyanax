package com.netflix.astyanax.cql.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.cql.test.ClusterConfiguration.Driver;

public class CqlClusterImplTest {
	   
	private static AstyanaxContext<Cluster> context;
    private static Cluster cluster;
    
    @BeforeClass
    public static void setup() throws Exception {
        
    	System.out.println("TESTING CQL KEYSPACE");
        //Thread.sleep(CASSANDRA_WAIT_TIME);

    	context = AstyanaxContextFactory.getCluster(Driver.JAVA_DRIVER);
        context.start();
        
        cluster = context.getClient();
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (context != null)
            context.shutdown();
        
        //Thread.sleep(CASSANDRA_WAIT_TIME);
    }

    @Test
    public void test() throws Exception {
    	
    	SchemaTests schemaTests = new SchemaTests(context, cluster, Driver.JAVA_DRIVER);
    	schemaTests.testCreateKeyspaceAndVerifyProperties();
    	schemaTests.testCreateColumnFamilyAndVerifyProperties();
    }

}

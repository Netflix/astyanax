package com.netflix.astyanax.cql.test;

import java.util.Properties;

import junit.framework.Assert;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.cql.test.ClusterConfiguration.Driver;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;

public class SchemaTests {

	private final AstyanaxContext<Cluster> context;
    private final Cluster cluster;

    private final Driver driver; 
    
    private final String keyspaceName = "clustertest";

    public SchemaTests(AstyanaxContext<Cluster> context, Cluster cluster, Driver driver) {
    	this.context = context;
    	this.cluster = cluster;
    	this.driver = driver;
    }
	
	public void testCreateKeyspaceAndVerifyProperties() throws Exception {
        
        Properties props = new Properties();
        props.put("name",                                keyspaceName);
        props.put("strategy_class",                      "SimpleStrategy");
        props.put("strategy_options.replication_factor", "1");
        
    	System.out.println("CREATING KEYSPACE " + keyspaceName);

        //cluster.createKeyspace(props);
        
        Properties prop1 = cluster.getKeyspaceProperties(keyspaceName);
        System.out.println(prop1);
        Assert.assertTrue(prop1.containsKey("name"));
        Assert.assertTrue(prop1.containsKey("strategy_class") || prop1.containsKey("replication.class"));
	
        Properties prop2 = cluster.getAllKeyspaceProperties();
        System.out.println(prop2);
        Assert.assertTrue(prop2.containsKey("clustertest.name"));
        
        if (driver == Driver.JAVA_DRIVER) {
            Assert.assertTrue(prop2.containsKey("clustertest.replication.class"));
        } else {
            Assert.assertTrue(prop2.containsKey("clustertest.strategy_class"));
        }
	}
	
	public void testCreateColumnFamilyAndVerifyProperties() throws Exception {
        Properties cfProps = new Properties();
        cfProps.put("keyspace",   keyspaceName);
        cfProps.put("name",       "cf1");
        cfProps.put("read_repair_chance", "0.10");
        
        //cluster.createColumnFamily(cfProps);
        
        Properties cfProps1 = cluster.getKeyspaceProperties(keyspaceName);
        KeyspaceDefinition ksdef = cluster.describeKeyspace(keyspaceName);
        ColumnFamilyDefinition cfdef = ksdef.getColumnFamily("cf1");
        
        if (driver == Driver.JAVA_DRIVER) {
            Assert.assertEquals(cfdef.getProperties().get("key_validator"), "org.apache.cassandra.db.marshal.BytesType");
        } else {
            Assert.assertEquals(cfdef.getProperties().get("cf_defs.cf1.comparator_type"), "org.apache.cassandra.db.marshal.BytesType");
        }
	}
}

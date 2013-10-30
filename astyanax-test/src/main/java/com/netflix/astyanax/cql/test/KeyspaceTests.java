package com.netflix.astyanax.cql.test;

import org.apache.log4j.PropertyConfigurator;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.cql.test.utils.AstyanaxContextFactory;

public class KeyspaceTests {
	
	public static AstyanaxContext<Keyspace> context;
	public static Keyspace keyspace;
    
	public KeyspaceTests() {
	}
	
    public static void initContext() throws Exception {
    	PropertyConfigurator.configure("./src/main/java/test-log4j.properties");
    	keyspace = AstyanaxContextFactory.getCachedKeyspace();
    }
}

package com.netflix.astyanax.cql.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.cql.test.ClusterConfiguration.Driver;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class KeyspaceTests {

	private static final Logger LOG = LoggerFactory.getLogger(KeyspaceTests.class);
	
	protected final AstyanaxContext<Keyspace> context;
	protected final Keyspace keyspace;
	protected final Driver driver; 
    
    public KeyspaceTests(AstyanaxContext<Keyspace> context, Keyspace keyspace, Driver driver) {
    	this.context = context;
    	this.keyspace = keyspace;
    	this.driver = driver;
    }

    
    public <T> void logColumnList(String label, ColumnList<T> cl) {
        LOG.info(">>>>>> " + label);
        for (Column<T> c : cl) {
        	if (driver == Driver.JAVA_DRIVER) {
        		LOG.info(" " + c.getName());
        	} else {
        		LOG.info(c.getName() + " " + c.getTimestamp());
        	}
        }
        LOG.info("<<<<<<");
    }
    
}

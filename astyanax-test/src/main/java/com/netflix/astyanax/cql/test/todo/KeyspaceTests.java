package com.netflix.astyanax.cql.test.todo;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.cql.test.utils.AstyanaxContextFactory;
import com.netflix.astyanax.cql.test.utils.ClusterConfiguration;
import com.netflix.astyanax.cql.test.utils.ClusterConfiguration.Driver;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class KeyspaceTests {

	private static final Logger LOG = LoggerFactory.getLogger(KeyspaceTests.class);
	
	public static AstyanaxContext<Keyspace> context;
	public static Keyspace keyspace;
    
	public KeyspaceTests() {
		
	}
	
    public <T> void logColumnList(String label, ColumnList<T> cl) {
        LOG.info(">>>>>> " + label);
        for (Column<T> c : cl) {
        	if (ClusterConfiguration.TheDriver == Driver.JAVA_DRIVER) {
        		LOG.info(" " + c.getName());
        	} else {
        		LOG.info(c.getName() + " " + c.getTimestamp());
        	}
        }
        LOG.info("<<<<<<");
    }
    
    public static void initContext() throws Exception {
    	PropertyConfigurator.configure("./src/main/java/test-log4j.properties");

    	context = AstyanaxContextFactory.getKeyspace();
    	context.start();
        keyspace = context.getClient();
    }
    
}

package com.netflix.astyanax.cql.test.utils;


/**
 * Configuration class that dictates what Driver to use under the covers for the Astyanax Client.
 * Users can explicitly set the driver to use - default if JAVA_DRIVER. 
 * 
 * Note that this class provides helpful utilities to setup the AstyanaxContext using the 
 * specified driver. 
 * 
 * @author poberai
 *
 */
public class ClusterConfiguration {
	
    public static String TEST_CLUSTER_NAME  = "Test Cluster"; // use cass_sandbox
    public static String TEST_KEYSPACE_NAME = "astyanaxunittests";
    
    public static Driver TheDriver = Driver.JAVA_DRIVER;
    //public static Driver TheDriver = Driver.THRIFT;
    
    public static enum Driver {
    	THRIFT, JAVA_DRIVER; 
    }
    
    public static void setDriver(Driver driver) {
    	TheDriver = driver;
    }

    public static Driver getDriver() {
    	return TheDriver;
    }
}

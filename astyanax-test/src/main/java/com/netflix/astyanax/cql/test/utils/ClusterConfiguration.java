/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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

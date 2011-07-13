package com.netflix.astyanax.mock;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnType;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class MockConstants {

	public static ColumnFamily<String, String> CF_STANDARD1 = 
		new ColumnFamily<String, String>("Standard1", 		StringSerializer.get(), StringSerializer.get(), ColumnType.STANDARD);

	public static ColumnFamily<String, String> CF_SUPER1    = 
		new ColumnFamily<String, String>("Super1",    		StringSerializer.get(), StringSerializer.get(), ColumnType.SUPER);
	
	public static ColumnFamily<String, String> CF_COUNTER1    = 
		new ColumnFamily<String, String>("Counter1",  		StringSerializer.get(), StringSerializer.get(), ColumnType.STANDARD);
    
	public static ColumnFamily<String, String> CF_COUNTER_SUPER1    = 
		new ColumnFamily<String, String>("CounterSuper1",  	StringSerializer.get(), StringSerializer.get(), ColumnType.SUPER);
	
	public static ColumnFamily<String, String> CF_NOT_DEFINED    = 
		new ColumnFamily<String, String>("NotDefined",  	StringSerializer.get(), StringSerializer.get(), ColumnType.STANDARD);
	
	public static ColumnFamily<String, Composite> CF_COMPOSITE    = 
		new ColumnFamily<String, Composite>("Composite1",  	StringSerializer.get(), new CompositeSerializer());
	
	public static final String CLUSTER_NAME = "TestCluster1";
	public static final String KEYSPACE_NAME = "Keyspace1";
	public static final int DAEMON_START_WAIT = 3000;
	public static final int PORT = 7102;
	public static final int DATA_PORT = 7101;
	public static final boolean ENABLED = true;
   
}

package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.Host;

public interface KeyspaceTracers {
	void incMutation(Keyspace keyspace, Host host, long latency);
	
	void incRowQuery(Keyspace keyspace, Host host, long latency);
	
	void incMultiRowQuery(Keyspace keyspace, Host host, long latency);
	
	void incIndexQuery(Keyspace keyspace, Host host, long latency);
}

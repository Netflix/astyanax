package com.netflix.astyanax.shallows;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.KeyspaceTracers;
import com.netflix.astyanax.connectionpool.Host;

public class EmptyKeyspaceTracers implements KeyspaceTracers {

	@Override
	public void incMutation(Keyspace keyspace, Host host, long latency) {
	}

	@Override
	public void incRowQuery(Keyspace keyspace, Host host, long latency) {
	}

	@Override
	public void incMultiRowQuery(Keyspace keyspace, Host host, long latency) {
	}

	@Override
	public void incIndexQuery(Keyspace keyspace, Host host, long latency) {
	}

}

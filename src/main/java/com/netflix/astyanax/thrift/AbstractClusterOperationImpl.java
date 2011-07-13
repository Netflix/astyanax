package com.netflix.astyanax.thrift;

import java.math.BigInteger;

import org.apache.cassandra.thrift.Cassandra;

import com.netflix.astyanax.connectionpool.Operation;

public abstract class AbstractClusterOperationImpl<R> implements Operation<Cassandra.Client, R> {
	@Override
	public BigInteger getKey() {
		return null;
	}

	@Override
	public String getKeyspace() {
		return null;
	}
	

}

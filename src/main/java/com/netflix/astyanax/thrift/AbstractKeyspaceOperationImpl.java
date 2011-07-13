package com.netflix.astyanax.thrift;

import java.math.BigInteger;

import org.apache.cassandra.thrift.Cassandra;

import com.netflix.astyanax.connectionpool.Operation;

public abstract class AbstractKeyspaceOperationImpl<R> implements Operation<Cassandra.Client, R>{
	private String keyspaceName;
	
	public AbstractKeyspaceOperationImpl(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	@Override
	public String getKeyspace() {
		return this.keyspaceName;
	}
	
	@Override
	public BigInteger getKey() {
		return null;
	}
}

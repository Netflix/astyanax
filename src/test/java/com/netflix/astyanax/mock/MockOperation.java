package com.netflix.astyanax.mock;

import java.math.BigInteger;

import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;

public class MockOperation implements Operation<MockClient, String> {
	@Override
	public String execute(MockClient client)
			throws ConnectionException, OperationException {
		return "RESULT";
	}

	@Override
	public BigInteger getKey() {
		return null;
	}

	@Override
	public String getKeyspace() {
		return null;
	}
}

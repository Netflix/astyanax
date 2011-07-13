package com.netflix.astyanax.connectionpool;

import java.math.BigInteger;
import java.util.concurrent.Future;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public interface AsyncOperation<CL, R> {
	Future<R> execute(CL client) throws ConnectionException;

	BigInteger getKey();
}

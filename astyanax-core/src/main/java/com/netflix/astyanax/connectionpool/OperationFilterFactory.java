package com.netflix.astyanax.connectionpool;

public interface OperationFilterFactory {
    <R, CL> Operation<R, CL> attachFilter(Operation<R,CL> operation);
}

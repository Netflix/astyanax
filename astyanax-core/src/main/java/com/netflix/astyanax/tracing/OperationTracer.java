package com.netflix.astyanax.tracing;

import com.netflix.astyanax.connectionpool.Operation;

public interface OperationTracer {
	
	public AstyanaxContext getAstyanaxContext();
	
	public <CL, R>  void onCall(AstyanaxContext ctx, Operation<CL, R> op);
	
	public <CL, R> void onSuccess(AstyanaxContext ctx, Operation<CL, R> op);

	public <CL, R> void onException(AstyanaxContext ctx, Operation<CL, R> op, Throwable t);

}

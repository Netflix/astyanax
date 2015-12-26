package com.netflix.astyanax.shallows;

import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.tracing.AstyanaxContext;
import com.netflix.astyanax.tracing.OperationTracer;

public class EmptyOperationTracer implements OperationTracer {

	@Override
	public AstyanaxContext getAstyanaxContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <CL, R> void onCall(AstyanaxContext ctx, Operation<CL, R> op) {
		// TODO Auto-generated method stub
	}

	@Override
	public <CL, R> void onSuccess(AstyanaxContext ctx, Operation<CL, R> op) {
		// TODO Auto-generated method stub	
	}

	@Override
	public <CL, R> void onException(AstyanaxContext ctx, Operation<CL, R> op, Throwable t) {
		// TODO Auto-generated method stub
	}

}

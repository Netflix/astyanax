/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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

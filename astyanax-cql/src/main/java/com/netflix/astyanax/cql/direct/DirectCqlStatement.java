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
package com.netflix.astyanax.cql.direct;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlOperationResultImpl;
import com.netflix.astyanax.cql.CqlPreparedStatement;
import com.netflix.astyanax.cql.CqlStatement;
import com.netflix.astyanax.cql.CqlStatementResult;
import com.netflix.astyanax.cql.util.AsyncOperationResult;
import com.netflix.astyanax.cql.util.ConsistencyLevelTransform;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Impl of {@link CqlStatement} using java driver.
 * it manages a {@link Session} object that is used when actually performing the real query with the 
 * driver underneath. 
 * 
 * @author poberai
 */
public class DirectCqlStatement implements CqlStatement {

	private final Session session; 
	
	private ConsistencyLevel cLevel = ConsistencyLevel.CL_ONE; // the default cl 
	private String cqlQuery; 
	
	public DirectCqlStatement(Session session) {
		this.session = session;
	}
	
	@Override
	public CqlStatement withConsistencyLevel(ConsistencyLevel cl) {
		this.cLevel = cl;
		return this;
	}

	@Override
	public CqlStatement withCql(String cql) {
		this.cqlQuery = cql;
		return this;
	}

	@Override
	public OperationResult<CqlStatementResult> execute() throws ConnectionException {
		
		Statement q = new SimpleStatement(cqlQuery);
		q.setConsistencyLevel(ConsistencyLevelTransform.getConsistencyLevel(cLevel));
		q.setFetchSize(100);
		ResultSet resultSet = session.execute(q);
		
		CqlStatementResult result = new DirectCqlStatementResultImpl(resultSet);
		return new CqlOperationResultImpl<CqlStatementResult>(resultSet, result);
	}

	@Override
	public ListenableFuture<OperationResult<CqlStatementResult>> executeAsync() throws ConnectionException {
		Statement q = new SimpleStatement(cqlQuery);
		q.setConsistencyLevel(ConsistencyLevelTransform.getConsistencyLevel(cLevel));

		ResultSetFuture rsFuture = session.executeAsync(q);

		return new AsyncOperationResult<CqlStatementResult>(rsFuture) {

			@Override
			public OperationResult<CqlStatementResult> getOperationResult(ResultSet rs) {
				CqlStatementResult result = new DirectCqlStatementResultImpl(rs);
				return new CqlOperationResultImpl<CqlStatementResult>(rs, result);			}
		};
	}

	@Override
	public CqlPreparedStatement asPreparedStatement() {
		PreparedStatement pStmt = session.prepare(cqlQuery);
		pStmt.setConsistencyLevel(ConsistencyLevelTransform.getConsistencyLevel(cLevel));
		return new DirectCqlPreparedStatement(session, pStmt);
	}

}

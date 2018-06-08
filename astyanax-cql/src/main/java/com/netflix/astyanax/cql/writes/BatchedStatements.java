/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.cql.writes;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class BatchedStatements {

	private static final Logger LOG = LoggerFactory.getLogger(BatchedStatements.class);
	
	private List<String> batchQueries = new ArrayList<String>(); 
	private List<Object> batchValues = new ArrayList<Object>();
	
	public BatchedStatements() {
	}
	
	public List<String> getBatchQueries() {
		return this.batchQueries; 
	}
	
	public List<Object> getBatchValues() {
		return this.batchValues;
	}
	
	public void addBatchQuery(String query) {
		batchQueries.add(query);
	}
	
	public void addBatchValues(List<Object> values) {
		batchValues.addAll(values);
	}
	
	public void addBatchValues(Object ... values) {
		for (Object value : values) {
			batchValues.add(value);
		}
	}

	public void addBatch(String query, Object ... values) {
		batchQueries.add(query);
		for (Object value : values) {
			batchValues.add(value);
		}
	}

	public void addBatch(String query, List<Object> values) {
		batchQueries.add(query);
		batchValues.addAll(values);
	}
	
	public void addBatch(BatchedStatements otherBatch) {
		batchQueries.addAll(otherBatch.getBatchQueries());
		batchValues.addAll(otherBatch.getBatchValues());
	}

	public BoundStatement getBoundStatement(Session session, boolean atomicBatch) {
		
		String query = getBatchQuery(atomicBatch);
		PreparedStatement statement = session.prepare(query);
		
		BoundStatement boundStatement = new BoundStatement(statement);

		Object[] valueArr = batchValues.toArray();
		boundStatement.bind(valueArr);
		
		return boundStatement;
	}
	
	public String getBatchQuery(boolean atomicBatch) {
		StringBuilder sb = new StringBuilder();
		
		boolean isBatch = batchQueries.size() > 1;
		
		if (isBatch) {
			if (atomicBatch) {
				sb.append("BEGIN BATCH ");
			} else {
				sb.append("BEGIN UNLOGGED BATCH ");
			}
		}
		for (String query : batchQueries) {
			sb.append(query);
		}
		
		if (isBatch) {
			sb.append(" APPLY BATCH; ");
		}
		
		String query = sb.toString(); 
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Query : " + query);
			LOG.debug("Bind values: " + batchValues);
		}
		return query;
	}
}

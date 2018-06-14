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
package com.netflix.astyanax.cql.direct;

import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.cql.CqlSchema;
import com.netflix.astyanax.cql.CqlStatementResult;
import com.netflix.astyanax.cql.reads.model.CqlRowListImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Rows;

/**
 * Impl of {@link CqlStatementResult} that parses the result set from java driver query operations. 
 * 
 * @author poberai
 */
public class DirectCqlStatementResultImpl implements CqlStatementResult {
	
	private final ResultSet rs; 
	
	public DirectCqlStatementResultImpl(ResultSet rs) {
		this.rs = rs;
	}

	@Override
	public long asCount() {
		return rs.one().getLong(0);
	}

	@Override
	public <K, C> Rows<K, C> getRows(ColumnFamily<K, C> columnFamily) {

		List<com.datastax.driver.core.Row> rows = rs.all(); 
		return new CqlRowListImpl<K, C>(rows, columnFamily);
	}

	@Override
	public CqlSchema getSchema() {
		return new DirectCqlSchema(rs);
	}
	
	public static class DirectCqlSchema implements CqlSchema {
		
		private final ResultSet rs;
		
		public DirectCqlSchema(ResultSet result) {
			this.rs = result;
		}
		
		public ResultSet getResultSet() {
			return rs;
		}
	}
}

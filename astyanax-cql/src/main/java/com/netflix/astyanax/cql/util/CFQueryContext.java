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
package com.netflix.astyanax.cql.util;

import java.nio.ByteBuffer;

import com.netflix.astyanax.cql.schema.CqlColumnFamilyDefinitionImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

public class CFQueryContext<K,C> {

	private final ColumnFamily<K,C> columnFamily;
	private final Object rowKey;
	private RetryPolicy retryPolicy;
	private ConsistencyLevel clLevel; 

	public CFQueryContext(ColumnFamily<K,C> cf) {
		this(cf, null, null, null);
	}

	public CFQueryContext(ColumnFamily<K,C> cf, K rKey) {
		this(cf, rKey, null, null);
	}

	public CFQueryContext(ColumnFamily<K,C> cf, K rKey, RetryPolicy retry, ConsistencyLevel cl) {
		this.columnFamily = cf;
		this.rowKey = checkRowKey(rKey);
		this.retryPolicy = retry;
		this.clLevel = cl;
	}

	public ColumnFamily<K, C> getColumnFamily() {
		return columnFamily;
	}

	public Object getRowKey() {
		return rowKey;
	}

	public void setRetryPolicy(RetryPolicy retry) {
		this.retryPolicy = retry;
	}

	public RetryPolicy getRetryPolicy() {
		return retryPolicy;
	}

	public void setConsistencyLevel(ConsistencyLevel cl) {
		this.clLevel = cl;
	}

	public ConsistencyLevel getConsistencyLevel() {
		return clLevel;
	}
	
	public Object checkRowKey(K rKey) {
		
		if (rKey == null) {
			return null;
		}
		
		CqlColumnFamilyDefinitionImpl cfDef = (CqlColumnFamilyDefinitionImpl) columnFamily.getColumnFamilyDefinition();
		
		if (cfDef.getKeyValidationClass().contains("BytesType")) {
			
			// Row key is of type bytes. Convert row key to bytebuffer if needed
			if (rKey instanceof ByteBuffer) {
				return rKey;
			}
			
			return columnFamily.getKeySerializer().toByteBuffer(rKey);
		}
		
		// else just return the row key as is
		return rKey;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("CF=").append(columnFamily.getName());
		sb.append(" RowKey: ").append(rowKey);
		sb.append(" RetryPolicy: ").append(retryPolicy);
		sb.append(" ConsistencyLevel: ").append(clLevel);
		return sb.toString();
	}
}

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
package com.netflix.astyanax;

import com.netflix.astyanax.model.ColumnPath;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Base interface for performing a query.  This interface provides additional
 * customization to the getXXX query calls on CassandraClient.  
 * @author elandau
 *
 * @param <K>	keyType
 * @param <C> 	ColumnType
 * @param <R>	ResponseType
 */
@Deprecated
public interface Query<K, C, R> extends Execution<R> {
	/**
	 * Set the consistency level for this query
	 * @param consistencyLevel
	 */
	Query<K,C,R> setConsistencyLevel(ConsistencyLevel consistencyLevel);
	
	/**
	 * Set the timeout for this query.  Set a high timeout when expecting a large
	 * result set such as when querying a KeySlice.  
	 * @param timeout In milliseconds
	 */
	Query<K,C,R> setTimeout(long timeout);
	
	/**
	 * Set the path to a super column or column
	 * @param path
	 */
	Query<K,C,R> setColumnPath(ColumnPath<C> path);
	
	/**
	 * Set the slice of columns to be returned
	 * @param slice
	 */
	Query<K,C,R> setColumnSlice(ColumnSlice<C> slice);
}

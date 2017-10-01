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

import com.datastax.driver.core.ConsistencyLevel;

/**
 * Utility class for transforming Astyanx consistency level to java driver consistency level.
 * 
 * @author poberai
 *
 */
public class ConsistencyLevelTransform {

	public static ConsistencyLevel getConsistencyLevel(com.netflix.astyanax.model.ConsistencyLevel level) {
		
		ConsistencyLevel result = null;
		switch(level) {
		case CL_ONE:
			result = ConsistencyLevel.ONE;
			break;
		case CL_ALL:
			result = ConsistencyLevel.ALL;
			break;
		case CL_ANY:
			result = ConsistencyLevel.ANY;
			break;
		case CL_QUORUM:
			result = ConsistencyLevel.QUORUM;
			break;
		case CL_EACH_QUORUM:
			result = ConsistencyLevel.EACH_QUORUM;
			break;
		case CL_LOCAL_QUORUM:
			result = ConsistencyLevel.LOCAL_QUORUM;
			break;
		default:
			throw new RuntimeException("Consistency level not supported");
		}
		return result;
	}
}

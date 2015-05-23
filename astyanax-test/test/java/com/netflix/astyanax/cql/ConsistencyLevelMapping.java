package com.netflix.astyanax.cql;

import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Helper class for translating Astyanax consistency level to java driver consistency level
 * 
 * @author poberai
 */
public class ConsistencyLevelMapping {

	public static com.datastax.driver.core.ConsistencyLevel getCL(ConsistencyLevel cl) {
		
		switch (cl) {
		
		case CL_ONE:
			return com.datastax.driver.core.ConsistencyLevel.ONE;
		case CL_TWO:
			return com.datastax.driver.core.ConsistencyLevel.TWO;
		case CL_THREE:
			return com.datastax.driver.core.ConsistencyLevel.THREE;
		case CL_QUORUM:
			return com.datastax.driver.core.ConsistencyLevel.QUORUM;
		case CL_LOCAL_QUORUM:
			return com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
		case CL_EACH_QUORUM:
			return com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM;
		case CL_ALL:
			return com.datastax.driver.core.ConsistencyLevel.ALL;
		case CL_ANY:
			return com.datastax.driver.core.ConsistencyLevel.ANY;
		default:
			throw new RuntimeException("CL Level not recognized: " + cl.name());
		}
	}
}

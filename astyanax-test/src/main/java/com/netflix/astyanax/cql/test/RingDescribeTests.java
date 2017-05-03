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
package com.netflix.astyanax.cql.test;

import java.util.List;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.connectionpool.TokenRange;

public class RingDescribeTests extends KeyspaceTests {
	
	private static final Logger LOG = Logger.getLogger(RingDescribeTests.class);
	
    @BeforeClass
	public static void init() throws Exception {
		initContext();
	}
	
    @Test
    public void testDescribeRing() throws Exception {
    	// [TokenRangeImpl [startToken=0, endToken=0, endpoints=[127.0.0.1]]]
    	List<TokenRange> ring = keyspace.describeRing();
    	LOG.info(ring.toString());
    }
}

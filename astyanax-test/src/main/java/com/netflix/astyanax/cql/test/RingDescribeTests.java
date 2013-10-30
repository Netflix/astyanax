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

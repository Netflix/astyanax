package com.netflix.astyanax.cql.test;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.cql.test.todo.KeyspaceTests;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.StringSerializer;

public class DirectCqlTests extends KeyspaceTests {
	
	private static final Logger LOG = Logger.getLogger(DirectCqlTests.class);
	
    public static ColumnFamily<String, String> CF_DIRECT = ColumnFamily
            .newColumnFamily(
                    "cfdirect", 
                    StringSerializer.get(),
                    StringSerializer.get());

    @BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.createColumnFamily(CF_DIRECT, null);
    	CF_DIRECT.describe(keyspace);
	}

    @AfterClass
	public static void tearDown() throws Exception {
    	keyspace.dropColumnFamily(CF_DIRECT);
	}

    @Test
    public void testCql() throws Exception {
    	
        MutationBatch m = keyspace.prepareMutationBatch();

        for (char keyName = 'A'; keyName <= 'F'; keyName++) {
            String rowKey = Character.toString(keyName);
            ColumnListMutation<String> cfmStandard = m.withRow(CF_DIRECT, rowKey);
            for (char cName = 'a'; cName <= 'z'; cName++) {
                cfmStandard.putColumn(Character.toString(cName), (int) (cName - 'a') + 1, null);
            }
            m.execute();
        }

    	System.out.println("testCQL");
    	LOG.info("CQL Test");
    	OperationResult<CqlResult<String, String>> result = keyspace
    			.prepareQuery(CF_DIRECT)
    			.withCql("SELECT * FROM astyanaxunittests.cfdirect;").execute();
    	Assert.assertTrue(result.getResult().hasRows());

    	Assert.assertEquals(6, result.getResult().getRows().size());
    	Assert.assertFalse(result.getResult().hasNumber());
    	
    	Row<String, String> row;

    	row = result.getResult().getRows().getRow("A");
    	Assert.assertEquals("A", row.getKey());
    	Assert.assertEquals(26, row.getColumns().size());
    	
    	row = result.getResult().getRows().getRow("B");
    	Assert.assertEquals("B", row.getKey());
    	Assert.assertEquals(26, row.getColumns().size());

    	row = result.getResult().getRows().getRow("NonExistent");
    	Assert.assertNull(row);

    	for (Row<String, String> row1 : result.getResult().getRows()) {
    		LOG.info("KEY***: " + row1.getKey()); 
    	}
    }
}

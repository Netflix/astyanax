package com.netflix.astyanax.cql.test;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.query.CqlQuery;
import com.netflix.astyanax.query.PreparedCqlQuery;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class DirectCqlTests extends KeyspaceTests {
	
    public static ColumnFamily<Integer, String> CF_DIRECT = ColumnFamily
            .newColumnFamily(
                    "cfdirect", 
                    IntegerSerializer.get(),
                    StringSerializer.get());

    @BeforeClass
	public static void init() throws Exception {
		initContext();
		
		keyspace.prepareQuery(CF_DIRECT)
		        .withCql("CREATE TABLE astyanaxunittests.cfdirect ( key int, column1 text, value bigint, PRIMARY KEY (key) )")
		        .execute();
	}

    @AfterClass
	public static void tearDown() throws Exception {
		keyspace.prepareQuery(CF_DIRECT)
        .withCql("DROP TABLE astyanaxunittests.cfdirect")
        .execute();
	}

    @Test
    public void testCql() throws Exception {
    	
    	// INSERT VALUES 
    	CqlQuery<Integer, String> cqlQuery = keyspace
    	.prepareQuery(CF_DIRECT)
    	.withCql("INSERT INTO astyanaxunittests.cfdirect (key, column1, value) VALUES (?,?,?)");
    	
    	for (int i=0; i<10; i++) {
    		PreparedCqlQuery<Integer, String> pStatement = cqlQuery.asPreparedStatement();
    		pStatement.withIntegerValue(i).withStringValue(""+i).withLongValue(Long.valueOf(""+i)).execute();
    	}
    	
    	// TEST REGULAR CQL
    	OperationResult<CqlResult<Integer, String>> result = keyspace
    			.prepareQuery(CF_DIRECT)
    			.withCql("SELECT * FROM astyanaxunittests.cfdirect;").execute();
    	Assert.assertTrue(result.getResult().hasRows());

    	Assert.assertEquals(10, result.getResult().getRows().size());
    	Assert.assertFalse(result.getResult().hasNumber());
    	
    	for (int i=0; i<10; i++) {
    		
    		Row<Integer, String> row = result.getResult().getRows().getRow(i);
        	Assert.assertTrue(i == row.getKey());
        	Assert.assertEquals(3, row.getColumns().size());
        	
        	Integer key = row.getColumns().getIntegerValue("key", null);
        	Assert.assertTrue(Integer.valueOf(""+i) == key);

        	String column1 = row.getColumns().getStringValue("column1", null);
        	Assert.assertEquals(""+i, column1);
        	
        	Long value = row.getColumns().getLongValue("value", null);
        	Assert.assertTrue(Long.valueOf(""+i) == value);
    	}
    	
    	//  TEST CQL COUNT

    	result = keyspace
    			.prepareQuery(CF_DIRECT)
    			.withCql("SELECT count(*) FROM astyanaxunittests.cfdirect;").execute();
    	Assert.assertFalse(result.getResult().hasRows());
    	Assert.assertTrue(result.getResult().hasNumber());

    	Assert.assertTrue(10 == result.getResult().getNumber());
    }
}

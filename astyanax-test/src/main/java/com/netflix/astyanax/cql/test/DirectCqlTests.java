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
    
    public static ColumnFamily<String, String> CF_EMPTY_TABLE = ColumnFamily
            .newColumnFamily(
                    "empty_table", 
                    StringSerializer.get(),
                    StringSerializer.get(),
                    StringSerializer.get());


    @BeforeClass
	public static void init() throws Exception {
		initContext();
		
		keyspace.prepareQuery(CF_DIRECT)
        .withCql("CREATE TABLE astyanaxunittests.cfdirect ( key int, column1 text, value bigint, PRIMARY KEY (key) )")
        .execute();
		keyspace.prepareQuery(CF_EMPTY_TABLE)
        .withCql("CREATE TABLE astyanaxunittests.empty_table ( key text, column1 text, value text, PRIMARY KEY (key) )")
        .execute();
	}

    @AfterClass
	public static void tearDown() throws Exception {
		keyspace.prepareQuery(CF_DIRECT)
        .withCql("DROP TABLE astyanaxunittests.cfdirect")
        .execute();
		keyspace.prepareQuery(CF_EMPTY_TABLE)
        .withCql("DROP TABLE astyanaxunittests.empty_table")
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
    
    
    @Test 
    public void testEmptyTable() throws Exception {

    	CqlResult<String, String> result = keyspace.prepareQuery(CF_EMPTY_TABLE)
    	.withCql("select * from astyanaxunittests.empty_table where  key = 'blah'")
    	.execute()
    	.getResult();
    	
    	Assert.assertFalse(result.hasRows());
    	Assert.assertFalse(result.hasNumber());
    	Assert.assertTrue(0 == result.getRows().size());
    }
}

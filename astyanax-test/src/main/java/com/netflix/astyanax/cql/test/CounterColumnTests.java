package com.netflix.astyanax.cql.test;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

public class CounterColumnTests extends KeyspaceTests {
	
    public static ColumnFamily<String, String> CF_COUNTER1 = ColumnFamily
            .newColumnFamily(
                    "Counter1", 
                    StringSerializer.get(),
                    StringSerializer.get());

    @BeforeClass
	public static void init() throws Exception {
		initContext();
    	keyspace.createColumnFamily(CF_COUNTER1,ImmutableMap.<String, Object>builder()
                .put("default_validation_class", "CounterColumnType")
                .build());
		
		CF_COUNTER1.describe(keyspace);
	}

    @AfterClass
	public static void tearDown() throws Exception {
		initContext();
		keyspace.dropColumnFamily(CF_COUNTER1);
	}

    @Test
    public void testIncrementCounter() throws Exception {
        long baseAmount, incrAmount = 100;
        Column<String> column;

        column = keyspace.prepareQuery(CF_COUNTER1).getRow("CounterRow1").getColumn("MyCounter").execute().getResult();
        //Assert.assertNull(column);

        baseAmount = 0; 
        
        MutationBatch m = keyspace.prepareMutationBatch();
        m.withRow(CF_COUNTER1, "CounterRow1").incrementCounterColumn("MyCounter", incrAmount);
        m.execute();
//
//        column = keyspace.prepareQuery(CF_COUNTER1).getRow("CounterRow1").getColumn("MyCounter").execute().getResult();
//        Assert.assertNotNull(column);
//        Assert.assertEquals(baseAmount + incrAmount, column.getLongValue());
//
//        m = keyspace.prepareMutationBatch();
//        m.withRow(CF_COUNTER1, "CounterRow1").incrementCounterColumn("MyCounter", incrAmount);
//        m.execute();
//
//        column = keyspace.prepareQuery(CF_COUNTER1).getRow("CounterRow1").getColumn("MyCounter").execute().getResult();
//        Assert.assertNotNull(column);
//        Assert.assertEquals(column.getLongValue(), baseAmount + 2 * incrAmount);
    }

    @Test
    public void testDeleteCounter() throws Exception {
        Column<String> column;
        String rowKey = "CounterRowDelete1";
        String counterName = "MyCounter";

        // Increment the column
        MutationBatch m = keyspace.prepareMutationBatch();
        m.withRow(CF_COUNTER1, rowKey).incrementCounterColumn(counterName, 1);
        m.execute();

//        // Read back the value
//        column = keyspace.prepareQuery(CF_COUNTER1).getRow(rowKey).getColumn(counterName).execute().getResult();
//        Assert.assertNotNull(column);
//        Assert.assertEquals(column.getLongValue(), 1);
//
//        // Delete the column
//        keyspace.prepareColumnMutation(CF_COUNTER1, rowKey, counterName).deleteCounterColumn().execute();
//
//        // Try to read back
//        // This should be non-existent
//        column = keyspace.prepareQuery(CF_COUNTER1).getRow(rowKey).getColumn(counterName).execute().getResult();
//        Assert.assertNull(column);
    }


}

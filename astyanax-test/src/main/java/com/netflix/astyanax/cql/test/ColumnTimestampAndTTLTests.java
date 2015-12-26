package com.netflix.astyanax.cql.test;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class ColumnTimestampAndTTLTests extends KeyspaceTests {

    private static ColumnFamily<Long, Long> CF_COL_TIMESTAMP = ColumnFamily
            .newColumnFamily(
                    "columntimestamps", 
                    LongSerializer.get(),
                    LongSerializer.get(),
                    LongSerializer.get());
	
    private static ColumnFamily<String, String> CF_TTL = ColumnFamily
            .newColumnFamily(
                    "columnttls", 
                    StringSerializer.get(),
                    StringSerializer.get());

	@BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.createColumnFamily(CF_COL_TIMESTAMP,     null);
		keyspace.createColumnFamily(CF_TTL,     null);
		
		CF_COL_TIMESTAMP.describe(keyspace);
		CF_TTL.describe(keyspace);
	}
	
	@AfterClass
	public static void teardown() throws Exception {
		keyspace.dropColumnFamily(CF_COL_TIMESTAMP);
		keyspace.dropColumnFamily(CF_TTL);
	}

	@Test
	public void testColumnTimestamps() throws Exception {
		
		CF_COL_TIMESTAMP.describe(keyspace);

        MutationBatch mb = keyspace.prepareMutationBatch();
        mb.withRow(CF_COL_TIMESTAMP, 1L)
            .setTimestamp(1).putColumn(1L, 1L)
            .setTimestamp(10).putColumn(2L, 2L)
            ;
        mb.execute();
        
        ColumnList<Long> result1 = keyspace.prepareQuery(CF_COL_TIMESTAMP).getRow(1L).execute().getResult();
        Assert.assertEquals(2, result1.size());
        Assert.assertNotNull(result1.getColumnByName(1L));
        Assert.assertNotNull(result1.getColumnByName(2L));
        
        mb = keyspace.prepareMutationBatch();
        mb.withRow(CF_COL_TIMESTAMP,  1L)
            .setTimestamp(result1.getColumnByName(1L).getTimestamp()-1)
            .deleteColumn(1L)
            .setTimestamp(result1.getColumnByName(2L).getTimestamp()-1)
            .deleteColumn(2L)
            .putEmptyColumn(3L, null);
        
        mb.execute();
        
        result1 = keyspace.prepareQuery(CF_COL_TIMESTAMP).getRow(1L).execute().getResult();
        Assert.assertEquals(3, result1.size());
        
        mb = keyspace.prepareMutationBatch();
        mb.withRow(CF_COL_TIMESTAMP,  1L)
            .setTimestamp(result1.getColumnByName(1L).getTimestamp()+1)
            .deleteColumn(1L)
            .setTimestamp(result1.getColumnByName(2L).getTimestamp()+1)
            .deleteColumn(2L);
        mb.execute();
        
        result1 = keyspace.prepareQuery(CF_COL_TIMESTAMP).getRow(1L).execute().getResult();
        Assert.assertEquals(1, result1.size());
    }
	

    @Test
    public void testTtlValues() throws Exception {
        MutationBatch mb = keyspace.prepareMutationBatch();
        mb.withRow(CF_TTL, "row")
          .putColumn("TTL0", "TTL0", 0)
          .putColumn("TTLNULL", "TTLNULL", null)
          .putColumn("TTL1", "TTL1", 1);
        
        mb.execute();
        
        Thread.sleep(2000);
        
        ColumnList<String> result = keyspace.prepareQuery(CF_TTL)
            .getRow("row")
            .execute().getResult();
       
        Assert.assertEquals(2,  result.size());
        Assert.assertNotNull(result.getColumnByName("TTL0"));
        Assert.assertNotNull(result.getColumnByName("TTLNULL"));
    }
}

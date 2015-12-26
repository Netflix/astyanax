package com.netflix.astyanax.cql.test;

import java.util.UUID;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.reads.model.CqlRangeBuilder;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class TimeUUIDTests extends KeyspaceTests {
	
	private static final Logger LOG = Logger.getLogger(TimeUUIDTests.class);
	
    public static ColumnFamily<String, UUID> CF_TIME_UUID = ColumnFamily
            .newColumnFamily(
                    "TimeUUID1", 
                    StringSerializer.get(),
                    TimeUUIDSerializer.get());

    @BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.createColumnFamily(CF_TIME_UUID, null);
		CF_TIME_UUID.describe(keyspace);
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(CF_TIME_UUID);
	}

    @Test
    public void testTimeUUID() throws Exception {
    	
    	MutationBatch m = keyspace.prepareMutationBatch();

        UUID columnName = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        long columnTime = TimeUUIDUtils.getTimeFromUUID(columnName);
        String rowKey = "Key1";

        m.withRow(CF_TIME_UUID, rowKey).delete();
        m.execute();
        m.discardMutations();
        
        int startTime = 100;
        int endTime = 200;

        m.withRow(CF_TIME_UUID, rowKey).putColumn(columnName, 42, null);
        for (int i = startTime; i < endTime; i++) {
            // UUID c = TimeUUIDUtils.getTimeUUID(i);
            LOG.info(TimeUUIDUtils.getTimeUUID(columnTime + i).toString());

            m.withRow(CF_TIME_UUID, rowKey).putColumn(
                    TimeUUIDUtils.getTimeUUID(columnTime + i), i, null);
        }

        m.execute();

        OperationResult<Column<UUID>> result = keyspace
        		.prepareQuery(CF_TIME_UUID).getKey(rowKey)
        		.getColumn(columnName).execute();

        Assert.assertEquals(columnName, result.getResult().getName());
        Assert.assertTrue(result.getResult().getIntegerValue() == 42);

        OperationResult<ColumnList<UUID>> result2 = keyspace.prepareQuery(CF_TIME_UUID).getKey(rowKey).execute();
        Assert.assertTrue(result2.getResult().size() >= (endTime - startTime));

        result2 = keyspace
        		.prepareQuery(CF_TIME_UUID)
        		.getKey(rowKey)
        		.withColumnRange(
        				new RangeBuilder()
        				.setLimit(10)
        				.setStart(TimeUUIDUtils.getTimeUUID(0))
        				.setEnd(TimeUUIDUtils
        						.getTimeUUID(Long.MAX_VALUE >> 8))
        						.build()).execute();

        Assert.assertEquals(10, result2.getResult().size());

        // Test timeUUID pagination
        RowQuery<String, UUID> query = keyspace
                .prepareQuery(CF_TIME_UUID)
                .getKey(rowKey)
                .withColumnRange(
                        new CqlRangeBuilder<UUID>()
                                .setFetchSize(10)
                                .setStart(
                                        TimeUUIDUtils.getTimeUUID(columnTime
                                                + startTime))
                                .setEnd(TimeUUIDUtils.getTimeUUID(columnTime
                                        + endTime)).build()).autoPaginate(true);
        OperationResult<ColumnList<UUID>> result3;
        int pageCount = 0;
        int rowCount = 0;
        try {
            LOG.info("starting pagination");
            while (!(result3 = query.execute()).getResult().isEmpty()) {
                pageCount++;
                Assert.assertTrue(result3.getResult().size() <= 10);
                rowCount += result3.getResult().size();
                LOG.info("==== Block ====");
                for (Column<UUID> column : result3.getResult()) {
                    LOG.info("Column is " + column.getName());
                }
            }
            Assert.assertTrue("pagination complete:  " + pageCount, pageCount >= 10);
            Assert.assertTrue("pagination complete ", rowCount <= 100);
        } catch (ConnectionException e) {
            Assert.fail();
            LOG.info(e.getMessage());
            e.printStackTrace();
        }

    }

    @Test
    public void testTimeUUID2() throws Exception {
    	
    	CF_TIME_UUID.describe(keyspace);
    
        MutationBatch m = keyspace.prepareMutationBatch();
        String rowKey = "Key2";
        m.withRow(CF_TIME_UUID, rowKey).delete();
        m.execute();
        m.discardMutations();

        long now = System.currentTimeMillis();
        long msecPerDay = 86400000;
        for (int i = 0; i < 100; i++) {
            m.withRow(CF_TIME_UUID, rowKey).putColumn(
                    TimeUUIDUtils.getTimeUUID(now - i * msecPerDay), i, null);
        }
        m.execute();
        
        OperationResult<ColumnList<UUID>> result = keyspace
        		.prepareQuery(CF_TIME_UUID)
        		.getKey(rowKey)
        		.withColumnRange(
        				new RangeBuilder()
        				.setLimit(100)
        				.setStart(
        						TimeUUIDUtils.getTimeUUID(now - 20
        								* msecPerDay)).build())
        								.execute();
        Assert.assertTrue(result.getResult().size() >= 20);
    }
}

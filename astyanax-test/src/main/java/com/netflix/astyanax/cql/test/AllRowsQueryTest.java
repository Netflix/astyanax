package com.netflix.astyanax.cql.test;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.RowCallback;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;

public class AllRowsQueryTest extends KeyspaceTests {
    
	private static final Logger LOG = LoggerFactory.getLogger(AllRowsQueryTest.class);
	
	public static ColumnFamily<String, String> CF_ALL_ROWS = ColumnFamily
            .newColumnFamily(
                    "allrows", 
                    StringSerializer.get(),
                    StringSerializer.get());

    @BeforeClass
	public static void init() throws Exception {
		initContext();
		
		keyspace.createColumnFamily(CF_ALL_ROWS, null);
		CF_ALL_ROWS.describe(keyspace);
		
		/** POPULATE ROWS FOR TESTS */
    	MutationBatch m = keyspace.prepareMutationBatch();

        for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
            String rowKey = Character.toString(keyName);
            ColumnListMutation<String> cfmStandard = m.withRow(CF_ALL_ROWS, rowKey);
            for (char cName = 'a'; cName <= 'z'; cName++) {
                cfmStandard.putColumn(Character.toString(cName), (int) (cName - 'a') + 1, null);
            }
            m.withCaching(true);
            m.execute();
            m.discardMutations();
        }
    }

    @AfterClass
	public static void tearDown() throws Exception {
    	keyspace.dropColumnFamily(CF_ALL_ROWS);
    }
    
	@Test
	public void getAllWithCallback() throws Exception {
		
		final AtomicLong counter = new AtomicLong();

		keyspace.prepareQuery(CF_ALL_ROWS).getAllRows()
		.setRowLimit(30)
		.setRepeatLastToken(false)
		.setConcurrencyLevel(2)
		//.withColumnRange(new RangeBuilder().setLimit(2).build())
		.executeWithCallback(new RowCallback<String, String>() {
			@Override
			public void success(Rows<String, String> rows) {
				for (Row<String, String> row : rows) {
					LOG.info("ROW: " + row.getKey() + " "
							+ row.getColumns().size());
					counter.incrementAndGet();
				}
			}

			@Override
			public boolean failure(ConnectionException e) {
				LOG.error(e.getMessage(), e);
				return false;
			}
		});
		LOG.info("Read " + counter.get() + " keys");
		Assert.assertEquals(26,  counter.get());
	}
	
	@Test
	public void getAll() throws Exception {
		AtomicLong counter = new AtomicLong(0);

		OperationResult<Rows<String, String>> rows = keyspace
				.prepareQuery(CF_ALL_ROWS).getAllRows().setConcurrencyLevel(2).setRowLimit(30)
				//.withColumnRange(new RangeBuilder().setLimit(0).build())
				.setExceptionCallback(new ExceptionCallback() {
					@Override
					public boolean onException(ConnectionException e) {
						Assert.fail(e.getMessage());
						return true;
					}
				}).execute();
		for (Row<String, String> row : rows.getResult()) {
			counter.incrementAndGet();
			LOG.info("ROW: " + row.getKey() + " " + row.getColumns().size());
		}
		LOG.info("Read " + counter.get() + " keys");
		Assert.assertEquals(26, counter.get());
	}
}

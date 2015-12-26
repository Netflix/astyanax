package com.netflix.astyanax.cql.test.recipes;

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Function;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.cql.test.KeyspaceTests;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.partitioner.Murmur3Partitioner;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.serializers.StringSerializer;

public class AllRowsReaderTest extends KeyspaceTests {

	public static ColumnFamily<String, String> CF_ALL_ROWS_READER = ColumnFamily
            .newColumnFamily(
                    "allrowsreader", 
                    StringSerializer.get(),
                    StringSerializer.get());

    @BeforeClass
	public static void init() throws Exception {
		initContext();
		
		keyspace.createColumnFamily(CF_ALL_ROWS_READER, null);
		CF_ALL_ROWS_READER.describe(keyspace);
		
		/** POPULATE ROWS FOR TESTS */
    	MutationBatch m = keyspace.prepareMutationBatch();

        for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
            String rowKey = Character.toString(keyName);
            ColumnListMutation<String> cfmStandard = m.withRow(CF_ALL_ROWS_READER, rowKey);
            for (char cName = 'a'; cName <= 'z'; cName++) {
                cfmStandard.putColumn(Character.toString(cName), (int) (cName - 'a') + 1, null);
            }
            m.execute();
            m.discardMutations();
        }
    }

    @AfterClass
	public static void tearDown() throws Exception {
    	keyspace.dropColumnFamily(CF_ALL_ROWS_READER);
    }

	
	@Test
	public void testAllRowsReader() throws Exception {
		
		final AtomicInteger rowCount = new AtomicInteger(0);
		
		new AllRowsReader.Builder<String, String>(keyspace, CF_ALL_ROWS_READER)
				.withPageSize(100) // Read 100 rows at a time
				.withConcurrencyLevel(10) // Split entire token range into 10.  Default is by number of nodes.
				.withPartitioner(Murmur3Partitioner.get())
				.forEachPage(new Function<Rows<String, String>, Boolean>() {
					@Override
					public Boolean apply(Rows<String, String> rows) {
						// Process the row here ...
						// This will be called from multiple threads so make sure your code is thread safe
						for (Row<String, String> row : rows) {
							ColumnList<String> colList = row.getColumns();
							Assert.assertTrue("ColList: " + colList.size(), 26 == colList.size());
							rowCount.incrementAndGet();
						}
						return true;
					}
				})
				.build()
				.call();

		Assert.assertTrue("RowCount: " + rowCount.get(), 26 == rowCount.get());
	}
}

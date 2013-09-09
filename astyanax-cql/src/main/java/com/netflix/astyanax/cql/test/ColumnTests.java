package com.netflix.astyanax.cql.test;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.cql.test.ClusterConfiguration.Driver;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.LongSerializer;

public class ColumnTests extends KeyspaceTests {

	private static final Logger LOG = LoggerFactory.getLogger(ColumnTests.class);

	public static ColumnFamily<Long, Long> CF_DELETE = ColumnFamily.newColumnFamily(
			"deleteCF", 
			LongSerializer.get(),
			LongSerializer.get());

	public ColumnTests() {
	}

	public void testMultiColumDelete() throws Exception {

		/** NOT POSSIBLE DUE TO MISSING BATCH MUTATIONS FROM CQL */
		//keyspace.createColumnFamily(CF_DELETE, null);

        MutationBatch mb = keyspace.prepareMutationBatch();
        mb.withRow(CF_DELETE, 1L)
            .setTimestamp(1).putEmptyColumn(1L, null)
        	.setTimestamp(10).putEmptyColumn(2L, null);
        mb.execute();
        
        ColumnList<Long> result1 = keyspace.prepareQuery(CF_DELETE).getRow(1L).execute().getResult();
        Assert.assertEquals(2, result1.size());
        Assert.assertNotNull(result1.getColumnByName(1L));
        Assert.assertNotNull(result1.getColumnByName(2L));
        
        logColumnList("Insert", result1);
        
        mb = keyspace.prepareMutationBatch();
        mb.withRow(CF_DELETE,  1L)
        	.setTimestamp(0)
            .deleteColumn(1L)
            .setTimestamp(9)
            .deleteColumn(2L)
            .putEmptyColumn(3L, null);
        
        mb.execute();
        
        result1 = keyspace.prepareQuery(CF_DELETE).getRow(1L).execute().getResult();
        logColumnList("Delete with older timestamp", result1);
        
        // TODO fix timestamps; 
        if (ClusterConfiguration.getDriver() == Driver.JAVA_DRIVER) {
        	Assert.assertEquals(1, result1.size());
        } else {
        	Assert.assertEquals(3, result1.size());
        }
        
        LOG.info("Delete L2 with newer TS: ");
        mb.withRow(CF_DELETE,  1L)
            .setTimestamp(1000)
            .deleteColumn(1L)
            .setTimestamp(1000)
            .deleteColumn(2L);
        mb.execute();
        
        result1 = keyspace.prepareQuery(CF_DELETE).getRow(1L).execute().getResult();
        logColumnList("Delete with newer timestamp", result1);
        Assert.assertEquals(1, result1.size());
    }


}

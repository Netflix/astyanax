package com.netflix.astyanax.cql.test;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.test.MockCompositeTypeTests.MockCompositeType;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class CompositeKeyTests extends KeyspaceTests {

	private static final Logger LOG = Logger.getLogger(CompositeKeyTests.class);
	
	@BeforeClass
	public static void init() throws Exception {
		initContext();
		
		keyspace.createColumnFamily(CF_COMPOSITE_KEY, ImmutableMap.<String, Object>builder()
				.put("key_validation_class", "BytesType")
				.build());
		 
        CF_COMPOSITE_KEY.describe(keyspace);
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(CF_COMPOSITE_KEY);
	}

	private static AnnotatedCompositeSerializer<MockCompositeType> M_SERIALIZER 
    	= new AnnotatedCompositeSerializer<MockCompositeType>(MockCompositeType.class);

    private static ColumnFamily<MockCompositeType, String> CF_COMPOSITE_KEY 
    	= ColumnFamily.newColumnFamily("compositekey", M_SERIALIZER, StringSerializer.get());
	
    @Test
    public void testCompositeKey() {
        MockCompositeType key = new MockCompositeType("A", 1, 2, true, "B");
        MutationBatch m = keyspace.prepareMutationBatch();
        m.withRow(CF_COMPOSITE_KEY, key).putColumn("Test", "Value", null);
        try {
            m.execute();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        try {
            ColumnList<String> row = keyspace.prepareQuery(CF_COMPOSITE_KEY)
                    .getKey(key).execute().getResult();
            Assert.assertFalse(row.isEmpty());
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }
    }
}

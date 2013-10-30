package com.netflix.astyanax.cql.test;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class MockCompositeTypeTests extends KeyspaceTests {

	private static final Logger LOG = Logger.getLogger(MockCompositeTypeTests.class);
	
	@BeforeClass
	public static void init() throws Exception {
		initContext();
		
        keyspace.createColumnFamily(CF_COMPOSITE, ImmutableMap.<String, Object>builder()
                .put("comparator_type", "CompositeType(AsciiType, IntegerType, IntegerType, BytesType, UTF8Type)")
                .build());
		
		CF_COMPOSITE.describe(keyspace);
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		keyspace.dropColumnFamily(CF_COMPOSITE);
	}

	private static AnnotatedCompositeSerializer<MockCompositeType> M_SERIALIZER 
    	= new AnnotatedCompositeSerializer<MockCompositeType>(MockCompositeType.class);

    private static ColumnFamily<String, MockCompositeType> CF_COMPOSITE 
    	= ColumnFamily.newColumnFamily("mockcompositetype", StringSerializer.get(), M_SERIALIZER);
	
    
    @Test
    public void testComposite() throws Exception {
        String rowKey = "Composite1";

        boolean bool = false;
        MutationBatch m = keyspace.prepareMutationBatch();
        ColumnListMutation<MockCompositeType> mRow = m.withRow(CF_COMPOSITE, rowKey);
        int columnCount = 0;
        for (char part1 = 'a'; part1 <= 'b'; part1++) {
            for (int part2 = 0; part2 < 10; part2++) {
                for (int part3 = 10; part3 < 11; part3++) {
                    bool = !bool;
                    columnCount++;
                    mRow.putEmptyColumn(
                            new MockCompositeType(Character.toString(part1),
                                    part2, part3, bool, "UTF"), null);
                }
            }
        }
        m.execute();
        LOG.info("Created " + columnCount + " columns");

        OperationResult<ColumnList<MockCompositeType>> result;
        
        result = keyspace.prepareQuery(CF_COMPOSITE).getKey(rowKey).execute();
        Assert.assertEquals(columnCount,  result.getResult().size());
        for (Column<MockCompositeType> col : result.getResult()) {
        	LOG.info("COLUMN: " + col.getName().toString());
        }

        Column<MockCompositeType> column = keyspace.prepareQuery(CF_COMPOSITE).getKey(rowKey)
        		.getColumn(new MockCompositeType("a", 0, 10, true, "UTF"))
        		.execute().getResult();
        LOG.info("Got single column: " + column.getName().toString());
        Assert.assertNotNull(column);
        Assert.assertEquals("MockCompositeType[a,0,10,true,UTF]", column.getName().toString());

        LOG.info("Range builder");
        result = keyspace
        		.prepareQuery(CF_COMPOSITE)
        		.getKey(rowKey)
        		.withColumnRange(
        				M_SERIALIZER
        				.buildRange()
        				.withPrefix("a")
        				.greaterThanEquals(1)
        				.lessThanEquals(1)
        				.build()).execute();
        Assert.assertTrue(1 == result.getResult().size());
        for (Column<MockCompositeType> col : result.getResult()) {
        	LOG.info("COLUMN: " + col.getName().toString());
        	Assert.assertEquals("MockCompositeType[a,1,10,false,UTF]", col.getName().toString());
        }
    }
    
	public static class MockCompositeType {
	    @Component
	    private String stringPart;

	    @Component
	    private Integer intPart;

	    @Component
	    private Integer intPart2;

	    @Component
	    private boolean boolPart;

	    @Component
	    private String utf8StringPart;

	    public MockCompositeType() {

	    }

	    public MockCompositeType(String part1, Integer part2, Integer part3,
	            boolean boolPart, String utf8StringPart) {
	        this.stringPart = part1;
	        this.intPart = part2;
	        this.intPart2 = part3;
	        this.boolPart = boolPart;
	        this.utf8StringPart = utf8StringPart;
	    }

	    public MockCompositeType setStringPart(String part) {
	        this.stringPart = part;
	        return this;
	    }

	    public String getStringPart() {
	        return this.stringPart;
	    }

	    public MockCompositeType setIntPart1(int value) {
	        this.intPart = value;
	        return this;
	    }

	    public int getIntPart1() {
	        return this.intPart;
	    }

	    public MockCompositeType setIntPart2(int value) {
	        this.intPart2 = value;
	        return this;
	    }

	    public int getIntPart2() {
	        return this.intPart2;
	    }

	    public MockCompositeType setBoolPart(boolean boolPart) {
	        this.boolPart = boolPart;
	        return this;
	    }

	    public boolean getBoolPart() {
	        return this.boolPart;
	    }

	    public MockCompositeType setUtf8StringPart(String str) {
	        this.utf8StringPart = str;
	        return this;
	    }

	    public String getUtf8StringPart() {
	        return this.utf8StringPart;
	    }

	    public String toString() {
	        return new StringBuilder().append("MockCompositeType[")
	                .append(stringPart).append(',').append(intPart).append(',')
	                .append(intPart2).append(',').append(boolPart).append(',')
	                .append(utf8StringPart).append(']').toString();
	    }
	}
}

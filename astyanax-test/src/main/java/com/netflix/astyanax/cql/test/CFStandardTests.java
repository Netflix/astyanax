package com.netflix.astyanax.cql.test;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.ObjectSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;

public class CFStandardTests extends KeyspaceTests {
	
	private static final Logger LOG = Logger.getLogger(CFStandardTests.class);
	
    public static ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
            .newColumnFamily(
                    "Standard1", 
                    StringSerializer.get(),
                    StringSerializer.get());
    
    public static ColumnFamily<String, String> CF_STANDARD2 = ColumnFamily
            .newColumnFamily(
                    "Standard2", 
                    StringSerializer.get(),
                    StringSerializer.get());

    private static ColumnFamily<String, String> CF_USER_INFO = ColumnFamily.newColumnFamily(
            "UserInfo", // Column Family Name
            StringSerializer.get(), // Key Serializer
            StringSerializer.get()); // Column Serializer

    @BeforeClass
	public static void init() throws Exception {
    	initContext();
		
		keyspace.createColumnFamily(CF_STANDARD1, null);
		keyspace.createColumnFamily(CF_STANDARD2, null);
		keyspace.createColumnFamily(CF_USER_INFO, null);
    	
		CF_STANDARD1.describe(keyspace);
    	CF_STANDARD2.describe(keyspace);
    	CF_USER_INFO.describe(keyspace);
	}

    @AfterClass
	public static void tearDown() throws Exception {
    	keyspace.dropColumnFamily(CF_STANDARD1);
    	keyspace.dropColumnFamily(CF_STANDARD2);
    	keyspace.dropColumnFamily(CF_USER_INFO);
	}

    @Test
    public void testSerializedClassValue() throws Exception {

    	UserInfo smo = new UserInfo();
    	smo.setLastName("Landau");
    	smo.setFirstName("Eran");

    	ByteBuffer bb = ObjectSerializer.get().toByteBuffer(smo);
    	
    	keyspace.prepareColumnMutation(CF_STANDARD1, "Key_SerializeTest",
    			"Column1").putValue(bb, null).execute();

    	UserInfo smo2 = (UserInfo) keyspace.prepareQuery(CF_STANDARD1)
    			.getKey("Key_SerializeTest").getColumn("Column1").execute()
    			.getResult().getValue(ObjectSerializer.get());
    	
    	Assert.assertEquals(smo, smo2);
    }    

    @Test
    public void testSingleOps() throws Exception {
    	
        String key = "SingleOpsTest";
        Random prng = new Random();

        // Set a string value
        {
            String column = "StringColumn";
            String value = RandomStringUtils.randomAlphanumeric(32);
            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .putValue(value, null).execute();
            
            // Read
            ColumnQuery<String> query = keyspace.prepareQuery(CF_STANDARD1).getKey(key).getColumn(column);
            
            String v = query.execute().getResult().getStringValue();
            Assert.assertEquals(value, v);

            // Delete
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .deleteColumn().execute();
            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult()
                        .getStringValue();
                Assert.fail();
            } catch (RuntimeException e) {
            } catch (NotFoundException e) {
            } catch (ConnectionException e) {
            	e.printStackTrace();
                Assert.fail();
            }
        } 
        
        // Set a byte value
        {
            String column = "ByteColumn";
            byte value = (byte) prng.nextInt(Byte.MAX_VALUE);
            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .putValue(value, null).execute();
            // Read
            byte v = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult().getByteValue();
            Assert.assertEquals(value, v);
            // Delete
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .deleteColumn().execute();
            // verify column gone
            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult().getByteValue();
                Assert.fail();
            } catch (NullPointerException e) {
            } catch (NotFoundException e) {
            	// expected
            }
        } 
        
        // Set a short value
        {
            String column = "ShortColumn";
            short value = (short) prng.nextInt(Short.MAX_VALUE);
            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .putValue(value, null).execute();
            
            // Read
            short v = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult().getShortValue();
            Assert.assertEquals(value, v);
            // Delete
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .deleteColumn().execute();
            // verify column gone
            try {
            	
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult().getShortValue();
                Assert.fail();
            } catch (NullPointerException e) {
            	// expected
            } catch (NotFoundException e) {
            	// expected
            }
        } 
        
        // Set a int value
        {
            String column = "IntColumn";
            int value = prng.nextInt();
            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .putValue(value, null).execute();
            // Read
            int v = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult().getIntegerValue();
            Assert.assertEquals(value, v);
            // Delete
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .deleteColumn().execute();
            // verify column gone
            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult().getIntegerValue();
                Assert.fail();
            } catch (NullPointerException e) {
            	// expected
            } catch (NotFoundException e) {
            	// expected
            }
        }
        
        // Set a long value
        {
            String column = "LongColumn";
            long value = prng.nextLong();
            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .putValue(value, null).execute();
            // Read
            long v = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult().getLongValue();
            Assert.assertEquals(value, v);
         // get as integer should fail
            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult()
                        .getIntegerValue();
                Assert.fail();
            } catch (Exception e) {
            	// expected
            }
            // Delete
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .deleteColumn().execute();
            // verify column gone
            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult().getLongValue();
                Assert.fail();
            } catch (NullPointerException e) {
            	// expected
            } catch (NotFoundException e) {
            	// expected
            }
        }
        
        // Set a float value
        {
            String column = "FloatColumn";
            float value = prng.nextFloat();
            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .putValue(value, null).execute();
            // Read
            float v = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult().getFloatValue();
            Assert.assertEquals(value, v);
            // Delete
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .deleteColumn().execute();
            // verify column gone
            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult().getFloatValue();
                Assert.fail();
            } catch (NullPointerException e) {
            	// expected
            } catch (NotFoundException e) {
            	// expected
            }
        }

        // Set a double value
        {
            String column = "DoubleColumn";
            double value = prng.nextDouble();
            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .putValue(value, null).execute();
            // Read
            double v = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult().getDoubleValue();
            Assert.assertEquals(value, v);
            // get as integer should fail
            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult()
                        .getIntegerValue();
                Assert.fail();
            } catch (Exception e) {
            	// expected
            }
            // Delete
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .deleteColumn().execute();
            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult()
                        .getDoubleValue();
                Assert.fail();
            } catch (NullPointerException e) {
            	// expected
            } catch (NotFoundException e) {
            } catch (ConnectionException e) {
                Assert.fail();
            }
        } 
        
        // Set long column with timestamp
        {
            String column = "TimestampColumn";
            long value = prng.nextLong();

            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .withTimestamp(100)
                    .putValue(value, null)
                    .execute();

            // Read
            Column<String> c = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult();
            Assert.assertEquals(100,  c.getTimestamp());
        }
    }
    
    
    @Test
    public void testEmptyColumn() {
        ColumnListMutation<String> mutation = keyspace.prepareMutationBatch().withRow(CF_STANDARD1, "ABC");
        
        try {
            mutation.putColumn(null,  1L);
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info(e.getMessage());
        }
        
        try {
            mutation.putColumn("",  1L);
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info(e.getMessage());
        }

        try {
            mutation.deleteColumn("");
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info(e.getMessage());
        }
        
        try {
            mutation.deleteColumn(null);
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testCqlCount() throws Exception {
    	LOG.info("CQL Test");
    	OperationResult<CqlResult<String, String>> result = keyspace
    			.prepareQuery(CF_STANDARD1)
    			.withCql("SELECT count(*) FROM astyanaxunittests.standard1 where KEY='A';")
    			.execute();

    	long count = result.getResult().getNumber();
    	LOG.info("CQL Count: " + count);
    	Assert.assertTrue(0 <= count);
    }

    @Test
    public void testGetSingleColumn() throws Exception {
    	
    	keyspace.prepareColumnMutation(CF_STANDARD1, "A", "a").putValue(1, null).execute();
        Column<String> column = keyspace.prepareQuery(CF_STANDARD1).getRow("A").getColumn("a").execute().getResult();
        Assert.assertNotNull(column);
        Assert.assertEquals(1, column.getIntegerValue());
    }

    @Test
    public void testGetSingleKeyNotExists() throws Exception {
        Column<String> column = keyspace.prepareQuery(CF_STANDARD1).getRow("AA").getColumn("ab").execute().getResult();
        Assert.assertNull(column);
    }
    
    @Test
    public void testFunctionalQuery() throws Exception {
    	MutationBatch m = keyspace.prepareMutationBatch();

        for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
            String rowKey = Character.toString(keyName);
            ColumnListMutation<String> cfmStandard = m.withRow(CF_STANDARD1, rowKey);
            for (char cName = 'a'; cName <= 'z'; cName++) {
                cfmStandard.putColumn(Character.toString(cName),
                        (int) (cName - 'a') + 1, null);
            }
            m.withCaching(true);
            m.execute();
            m.discardMutations();
        }
        
        OperationResult<ColumnList<String>> r1 = keyspace
                .prepareQuery(CF_STANDARD1).getKey("A").execute();
        Assert.assertTrue(26 <= r1.getResult().size());
    }
    
    @Test
    public void testNullKeyInMutation() throws Exception {
        try {
            keyspace.prepareMutationBatch().withRow(CF_STANDARD1,  null).putColumn("abc", "def");
            Assert.fail();
        }
        catch (Exception e) {
        }
    }
    
    @Test
    public void testColumnSlice() throws ConnectionException {
        OperationResult<ColumnList<String>> r1 = keyspace
                .prepareQuery(CF_STANDARD1).getKey("A")
                .withColumnSlice("a", "b").execute();
        Assert.assertEquals(2, r1.getResult().size());
    }
    
    @Test
    public void testColumnRangeSlice() throws ConnectionException {
        OperationResult<ColumnList<String>> r1 = keyspace
                .prepareQuery(CF_STANDARD1)
                .getKey("A")
                .withColumnRange(
                        new RangeBuilder().setStart("a").setEnd("b")
                                .setLimit(5).build()).execute();
        Assert.assertEquals(2, r1.getResult().size());

        OperationResult<ColumnList<String>> r2 = keyspace
                .prepareQuery(CF_STANDARD1).getKey("A")
                .withColumnRange("a", null, false, 5).execute();
        Assert.assertEquals(5, r2.getResult().size());
        Assert.assertEquals("a", r2.getResult().getColumnByIndex(0).getName());

        ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);
        OperationResult<ColumnList<String>> r3 = keyspace
                .prepareQuery(CF_STANDARD1).getKey("A")
                .withColumnRange(EMPTY_BUFFER, EMPTY_BUFFER, true, 5).execute();
        Assert.assertEquals(5, r3.getResult().size());
        Assert.assertEquals("z", r3.getResult().getColumnByIndex(0).getName());
    }
    

    @Test
    public void testGetSingleColumnNotExists() throws ConnectionException {
        Column<String> column = keyspace.prepareQuery(CF_STANDARD1).getRow("A").getColumn("DoesNotExist").execute().getResult();
        Assert.assertNull(column);
    }
    
    @Test
    public void testGetSingleColumnNotExistsAsync() {
        Future<OperationResult<Column<String>>> future = null;
        try {
            future = keyspace.prepareQuery(CF_STANDARD1).getKey("A")
                    .getColumn("DoesNotExist").executeAsync();
            future.get(1000, TimeUnit.MILLISECONDS);
        } catch (ConnectionException e) {
            LOG.info("ConnectionException: " + e.getMessage());
            Assert.fail();
        } catch (InterruptedException e) {
            LOG.info(e.getMessage());
            Assert.fail();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NotFoundException)
                LOG.info(e.getCause().getMessage());
            else {
                Assert.fail(e.getMessage());
            }
        } catch (TimeoutException e) {
            future.cancel(true);
            LOG.info(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testGetSingleKey() throws ConnectionException {
    	for (char key = 'A'; key <= 'Z'; key++) {
    		String keyName = Character.toString(key);
    		OperationResult<ColumnList<String>> result = keyspace.prepareQuery(CF_STANDARD1).getKey(keyName).execute();
    		Assert.assertNotNull(result.getResult());
    		Assert.assertFalse(result.getResult().isEmpty());
    	}
    }
    
    @Test
    public void testGetSingleKeyAsync() throws Exception {
    	Future<OperationResult<ColumnList<String>>> future = keyspace.prepareQuery(CF_STANDARD1).getKey("A").executeAsync();

    	ColumnList<String> result = future.get(1000, TimeUnit.MILLISECONDS).getResult();
    	Assert.assertFalse(result.isEmpty());
    }

    @Test
    public void testGetAllKeysRoot() throws ConnectionException {
    	LOG.info("Starting testGetAllKeysRoot...");

    	List<String> keys = new ArrayList<String>();
    	for (char key = 'A'; key <= 'Z'; key++) {
    		String keyName = Character.toString(key);
    		keys.add(keyName);
    	}

    	OperationResult<Rows<String, String>> result = keyspace
    			.prepareQuery(CF_STANDARD1)
    			.getKeySlice(keys.toArray(new String[keys.size()]))
    			.execute();

    	Assert.assertEquals(26,  result.getResult().size());

    	Row<String, String> row;

    	row = result.getResult().getRow("A");
    	Assert.assertEquals("A", row.getKey());

    	row = result.getResult().getRow("B");
    	Assert.assertEquals("B", row.getKey());

    	row = result.getResult().getRow("NonExistent");
    	Assert.assertNull(row);

    	row = result.getResult().getRowByIndex(10);
    	Assert.assertEquals("K", row.getKey());

    	LOG.info("... testGetAllKeysRoot");
    }
    
    @Test
    public void testEmptyRowKey() {
        try {
            keyspace.prepareMutationBatch().withRow(CF_STANDARD1, "");
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info(e.getMessage());
        }
        
        try {
            keyspace.prepareMutationBatch().withRow(CF_STANDARD1, null);
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testGetColumnSlice() throws ConnectionException {
    	LOG.info("Starting testGetColumnSlice...");
    	OperationResult<ColumnList<String>> result = keyspace
    			.prepareQuery(CF_STANDARD1)
    			.getKey("A")
    			.withColumnSlice(
    					new ColumnSlice<String>("c", "h").setLimit(5))
    					.execute();
    	Assert.assertNotNull(result.getResult());
    	Assert.assertEquals(5, result.getResult().size());
    }


    @Test
    public void testGetAllKeysPath() throws ConnectionException {
    	LOG.info("Starting testGetAllKeysPath...");

    	List<String> keys = new ArrayList<String>();
    	for (char key = 'A'; key <= 'Z'; key++) {
    		String keyName = Character.toString(key);
    		keys.add(keyName);
    	}

    	OperationResult<Rows<String, String>> result = keyspace
    			.prepareQuery(CF_STANDARD1)
    			.getKeySlice(keys.toArray(new String[keys.size()]))
    			.execute();

    	for (Row<String, String> row : result.getResult()) {
    		System.out.println(row.getColumns().size());
    	}

    	OperationResult<Map<String, Integer>> counts = keyspace
    			.prepareQuery(CF_STANDARD1)
    			.getKeySlice(keys.toArray(new String[keys.size()]))
    			.getColumnCounts()
    			.execute();

    	Assert.assertEquals(26, counts.getResult().size());

    	for (Entry<String, Integer> count : counts.getResult().entrySet()) {
    		Assert.assertEquals(new Integer(26), count.getValue());
    	}

    	LOG.info("Starting testGetAllKeysPath...");
    }

    public static class UserInfo implements Serializable {
        private static final long serialVersionUID = 6366200973810770033L;

        private String firstName;
        private String lastName;

        public UserInfo() {

        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getFirstName() {
            return this.firstName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public String getLastName() {
            return this.lastName;
        }

        public boolean equals(Object other) {
            UserInfo smo = (UserInfo) other;
            return firstName.equals(smo.firstName)
                    && lastName.equals(smo.lastName);
        }
    }

    @Test
    public void testHasValue() throws Exception {

    	MutationBatch m = keyspace.prepareMutationBatch();
    	m.withRow(CF_USER_INFO, "acct1234")
    	.putColumn("firstname", "john", null)
    	.putColumn("lastname", "smith", null)
    	.putColumn("address", "555 Elm St", null)
    	.putColumn("age", 30, null)
    	.putEmptyColumn("empty");

    	m.execute();
    	ColumnList<String> response = keyspace.prepareQuery(CF_USER_INFO).getRow("acct1234").execute().getResult();
    	Assert.assertEquals("firstname", response.getColumnByName("firstname").getName());
    	Assert.assertEquals("firstname", response.getColumnByName("firstname").getName());
    	Assert.assertEquals("john", response.getColumnByName("firstname").getStringValue());
    	Assert.assertEquals("john", response.getColumnByName("firstname").getStringValue());
    	Assert.assertEquals(true,  response.getColumnByName("firstname").hasValue());
    	Assert.assertEquals(false, response.getColumnByName("empty").hasValue());
    }

    @Test
    public void testDelete() throws Exception {
        LOG.info("Starting testDelete...");

        String rowKey = "DeleteMe_testDelete";

        MutationBatch m = keyspace.prepareMutationBatch();
        m.withRow(CF_STANDARD1, rowKey).putColumn("Column1", "X", null).putColumn("Column2", "X", null);
        m.execute();

        Column<String> column = keyspace.prepareQuery(CF_STANDARD1).getRow(rowKey).getColumn("Column1").execute().getResult();
        Assert.assertEquals("X", column.getStringValue());
        
        m = keyspace.prepareMutationBatch();
        m.withRow(CF_STANDARD1, rowKey).deleteColumn("Column1");
        m.execute();

        column = keyspace.prepareQuery(CF_STANDARD1).getRow(rowKey).getColumn("Column1").execute().getResult();
        Assert.assertNull(column);

        LOG.info("... testDelete");
    }

    @Test
    public void testDeleteLotsOfColumns() throws Exception {
        LOG.info("Starting testDelete...");

        String rowKey = "DeleteMe_testDeleteLotsOfColumns";

        int nColumns = 100;
        int pageSize = 25;

        // Insert a bunch of rows
        MutationBatch m = keyspace.prepareMutationBatch();
        ColumnListMutation<String> rm = m.withRow(CF_STANDARD1, rowKey);

        for (int i = 0; i < nColumns; i++) {
            rm.putEmptyColumn("" + i, null);
        }

        m.execute();
        
        // Verify count
        int count = keyspace.prepareQuery(CF_STANDARD1)
                    .setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
                    .getKey(rowKey).getCount().execute().getResult();
        Assert.assertEquals(nColumns, count);

        // Delete half of the columns
        m = keyspace.prepareMutationBatch().setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        rm = m.withRow(CF_STANDARD1, rowKey);

        for (int i = 0; i < nColumns / 2; i++) {
            rm.deleteColumn("" + i);
        }

        m.execute();
        
        // Verify count
        count = keyspace.prepareQuery(CF_STANDARD1)
                .setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
                .getKey(rowKey).getCount().execute().getResult();
        Assert.assertEquals(nColumns / 2, count);
        
        // GET ROW COUNT WITH PAGINATION
        RowQuery<String, String> query = keyspace.prepareQuery(CF_STANDARD1)
                .setConsistencyLevel(ConsistencyLevel.CL_QUORUM).getKey(rowKey)
                .withColumnRange(new RangeBuilder().setLimit(pageSize).build())
                .autoPaginate(true);

        ColumnList<String> result;
        count = 0;
        while (!(result = query.execute().getResult()).isEmpty()) {
            count += result.size();
        }

        Assert.assertEquals(nColumns / 2, count);

        // Delete all of the columns
        m = keyspace.prepareMutationBatch().setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        rm = m.withRow(CF_STANDARD1, rowKey);

        for (int i = 0; i < nColumns; i++) {
            rm.deleteColumn("" + i);
        }

        m.execute();
        
        // Verify count
        count = keyspace.prepareQuery(CF_STANDARD1)
                .setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
                .getKey(rowKey).getCount().execute().getResult();
        Assert.assertEquals(0, count);

        LOG.info("... testDelete");
    }
    
    @Test
    public void testCopy() throws ConnectionException {
    	
        String keyName = "A";

        keyspace.prepareQuery(CF_STANDARD1).getKey(keyName).copyTo(CF_STANDARD2, keyName).execute();

        ColumnList<String> list1 = keyspace.prepareQuery(CF_STANDARD1).getKey(keyName).execute().getResult();
        ColumnList<String> list2 = keyspace.prepareQuery(CF_STANDARD2).getKey(keyName).execute().getResult();

        Iterator<Column<String>> iter1 = list1.iterator();
        Iterator<Column<String>> iter2 = list2.iterator();

        while (iter1.hasNext()) {
        	Column<String> column1 = iter1.next();
        	Column<String> column2 = iter2.next();
        	Assert.assertEquals(column1.getName(), column2.getName());
        	Assert.assertEquals(column1.getByteBufferValue(),column2.getByteBufferValue());
        }
        Assert.assertFalse(iter2.hasNext());
    }
    
    @Test
    public void testMutationMerge() throws Exception {
        MutationBatch m1 = keyspace.prepareMutationBatch();
        MutationBatch m2 = keyspace.prepareMutationBatch();
        MutationBatch m3 = keyspace.prepareMutationBatch();
        MutationBatch m4 = keyspace.prepareMutationBatch();
        MutationBatch m5 = keyspace.prepareMutationBatch();

        m1.withRow(CF_STANDARD1, "1").putColumn("1", "X", null);
        m2.withRow(CF_STANDARD1, "2").putColumn("2", "X", null).putColumn("3", "X", null);
        m3.withRow(CF_STANDARD1, "3").putColumn("4", "X", null).putColumn("5", "X", null).putColumn("6", "X", null);
        m4.withRow(CF_STANDARD1, "1").putColumn("7", "X", null).putColumn("8", "X", null).putColumn("9", "X", null).putColumn("10", "X", null);

        MutationBatch merged = keyspace.prepareMutationBatch();

        Assert.assertEquals(merged.getRowCount(), 0);

        merged.mergeShallow(m1);
        Assert.assertEquals(merged.getRowCount(), 1);

        merged.mergeShallow(m2);
        Assert.assertEquals(merged.getRowCount(), 2);

        merged.mergeShallow(m3);
        Assert.assertEquals(merged.getRowCount(), 3);

        merged.mergeShallow(m4);
        Assert.assertEquals(merged.getRowCount(), 3);

        merged.mergeShallow(m5);
        Assert.assertEquals(merged.getRowCount(), 3);
        
        merged.execute();
        
        Rows<String, String> result = keyspace.prepareQuery(CF_STANDARD1).getRowSlice("1", "2", "3").execute().getResult();
        
        Assert.assertTrue(5 == result.getRow("1").getColumns().size());
        Assert.assertTrue(2 == result.getRow("2").getColumns().size());
        Assert.assertTrue(3 == result.getRow("3").getColumns().size());
    }

}

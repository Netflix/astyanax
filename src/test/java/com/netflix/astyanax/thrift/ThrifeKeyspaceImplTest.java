package com.netflix.astyanax.thrift;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import junit.framework.Assert;

import org.apache.cassandra.utils.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.RowCallback;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.impl.FilteringHostSupplier;
import com.netflix.astyanax.impl.RingDescribeHostSupplier;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnSlice;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.PreparedIndexExpression;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.recipes.locks.StaleLockException;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.recipes.uniqueness.DedicatedMultiRowUniquenessConstraint;
import com.netflix.astyanax.recipes.uniqueness.NotUniqueException;
import com.netflix.astyanax.retry.ExponentialBackoff;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.ObjectSerializer;
import com.netflix.astyanax.serializers.PrefixedSerializer;
import com.netflix.astyanax.serializers.SerializerPackageImpl;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import com.netflix.astyanax.serializers.UnknownComparatorException;
import com.netflix.astyanax.test.EmbeddedCassandra;
import com.netflix.astyanax.test.SessionEvent;
import com.netflix.astyanax.util.ColumnarRecordWriter;
import com.netflix.astyanax.util.CsvColumnReader;
import com.netflix.astyanax.util.CsvRecordReader;
import com.netflix.astyanax.util.JsonRowsWriter;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.RecordReader;
import com.netflix.astyanax.util.RecordWriter;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class ThrifeKeyspaceImplTest {

    private static Logger LOG = LoggerFactory.getLogger(ThrifeKeyspaceImplTest.class);

    private static Keyspace                  keyspace;
    private static AstyanaxContext<Keyspace> keyspaceContext;
    private static EmbeddedCassandra         cassandra;

    private static String TEST_CLUSTER_NAME  = "cass_sandbox";
    private static String TEST_KEYSPACE_NAME = "AstyanaxUnitTests";

    private static ColumnFamily<String, String> CF_USER_INFO = ColumnFamily.newColumnFamily(
            "Standard1", // Column Family Name
            StringSerializer.get(), // Key Serializer
            StringSerializer.get()); // Column Serializer

    private static ColumnFamily<Long, String> CF_USERS = ColumnFamily
            .newColumnFamily("users", LongSerializer.get(),
                    StringSerializer.get());

    public static ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
            .newColumnFamily("Standard1", StringSerializer.get(),
                    StringSerializer.get());

    public static ColumnFamily<String, Long> CF_LONGCOLUMN = ColumnFamily
            .newColumnFamily("LongColumn1", StringSerializer.get(),
                    LongSerializer.get());

    public static ColumnFamily<String, String> CF_STANDARD2 = ColumnFamily
            .newColumnFamily("Standard2", StringSerializer.get(),
                    StringSerializer.get());

    public static ColumnFamily<String, String> CF_COUNTER1 = ColumnFamily
            .newColumnFamily("Counter1", StringSerializer.get(),
                    StringSerializer.get());

    public static ColumnFamily<String, String> CF_NOT_DEFINED = ColumnFamily
            .newColumnFamily("NotDefined", StringSerializer.get(),
                    StringSerializer.get());

    public static ColumnFamily<String, String> CF_EMPTY = ColumnFamily
            .newColumnFamily("NotDefined", StringSerializer.get(),
                    StringSerializer.get());

    public static AnnotatedCompositeSerializer<MockCompositeType> M_SERIALIZER = new AnnotatedCompositeSerializer<MockCompositeType>(
            MockCompositeType.class);
    
    public static ColumnFamily<String, MockCompositeType> CF_COMPOSITE = ColumnFamily
            .newColumnFamily("CompositeColumn", StringSerializer.get(),
                    M_SERIALIZER);

    public static ColumnFamily<ByteBuffer, ByteBuffer> CF_COMPOSITE_CSV = ColumnFamily
            .newColumnFamily("CompositeCsv", ByteBufferSerializer.get(),
                    ByteBufferSerializer.get());

    public static ColumnFamily<MockCompositeType, String> CF_COMPOSITE_KEY = ColumnFamily
            .newColumnFamily("CompositeKey",
                    M_SERIALIZER, StringSerializer.get());

    public static ColumnFamily<String, UUID> CF_TIME_UUID = ColumnFamily
            .newColumnFamily("TimeUUID1", StringSerializer.get(),
                    TimeUUIDSerializer.get());

    public static ColumnFamily<String, UUID> CF_USER_UNIQUE_UUID = ColumnFamily
            .newColumnFamily("UserUniqueUUID", StringSerializer.get(),
                    TimeUUIDSerializer.get());
    
    public static ColumnFamily<String, UUID> CF_EMAIL_UNIQUE_UUID = ColumnFamily
            .newColumnFamily("EmailUniqueUUID", StringSerializer.get(),
                    TimeUUIDSerializer.get());
    
    public static AnnotatedCompositeSerializer<SessionEvent> SE_SERIALIZER = new AnnotatedCompositeSerializer<SessionEvent>(
            SessionEvent.class);

    public static ColumnFamily<String, SessionEvent> CF_CLICK_STREAM = ColumnFamily
            .newColumnFamily("ClickStream", StringSerializer.get(),
                    SE_SERIALIZER);

    private static ColumnFamily<String, String> LOCK_CF_LONG   = 
            ColumnFamily.newColumnFamily("LockCfLong", StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
    
    private static ColumnFamily<String, String> LOCK_CF_STRING = 
            ColumnFamily.newColumnFamily("LockCfString", StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
    
    private static final String SEEDS = "localhost:9160";

    private static final long   CASSANDRA_WAIT_TIME = 3000;
    private static final int    TTL                 = 20;
    private static final int    TIMEOUT             = 10;
    
    @BeforeClass
    public static void setup() throws Exception {
        System.out.println("TESTING THRIFT KEYSPACE");

        cassandra = new EmbeddedCassandra();
        cassandra.start();
        
        Thread.sleep(CASSANDRA_WAIT_TIME);
        
        createKeyspace();
    }

    @AfterClass
    public static void teardown() {
        if (keyspaceContext != null)
            keyspaceContext.shutdown();
        
        if (cassandra != null)
            cassandra.stop();
    }

    public static void createKeyspace() throws Exception {
        keyspaceContext = new AstyanaxContext.Builder()
                .forCluster(TEST_CLUSTER_NAME)
                .forKeyspace(TEST_KEYSPACE_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME
                                + "_" + TEST_KEYSPACE_NAME)
                                .setSocketTimeout(30000)
                                .setMaxTimeoutWhenExhausted(2000)
                                .setMaxConnsPerHost(10)
                                .setInitConnsPerHost(10)
                                .setSeeds(SEEDS))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        keyspaceContext.start();
        
        keyspace = keyspaceContext.getEntity();
        
        try {
            keyspace.dropKeyspace();
        }
        catch (Exception e) {
            
        }
        
        keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                        .put("replication_factor", "1")
                        .build())
                .put("strategy_class",     "SimpleStrategy")
                .build()
                );
        
        keyspace.createColumnFamily(CF_STANDARD1, ImmutableMap.<String, Object>builder()
                .put("column_metadata", ImmutableMap.<String, Object>builder()
                        .put("Index1", ImmutableMap.<String, Object>builder()
                                .put("validation_class", "UTF8Type")
                                .put("index_name",       "Index1")
                                .put("index_type",       "KEYS")
                                .build())
                        .put("Index2", ImmutableMap.<String, Object>builder()
                                .put("validation_class", "UTF8Type")
                                .put("index_name",       "Index2")
                                .put("index_type",       "KEYS")
                                .build())
                         .build())
                     .build());
        
        keyspace.createColumnFamily(CF_STANDARD2, null);
        keyspace.createColumnFamily(CF_LONGCOLUMN, null);
        keyspace.createColumnFamily(CF_COUNTER1, ImmutableMap.<String, Object>builder()
                .put("default_validation_class", "CounterColumnType")
                .build());
        keyspace.createColumnFamily(CF_CLICK_STREAM, ImmutableMap.<String, Object>builder()
                .put("comparator_type", "CompositeType(UTF8Type, TimeUUIDType)")
                .build());
        keyspace.createColumnFamily(CF_COMPOSITE_CSV, ImmutableMap.<String, Object>builder()
                .put("default_validation_class", "UTF8Type")
                .put("key_validation_class",     "UTF8Type")
                .put("comparator_type",          "CompositeType(UTF8Type, LongType)")
                .build());
        keyspace.createColumnFamily(CF_COMPOSITE, ImmutableMap.<String, Object>builder()
                .put("comparator_type", "CompositeType(AsciiType, IntegerType(reversed=true), IntegerType, BytesType, UTF8Type)")
                .build());
        keyspace.createColumnFamily(CF_COMPOSITE_KEY, ImmutableMap.<String, Object>builder()
                .put("key_validation_class", "BytesType")
                .build());
        keyspace.createColumnFamily(CF_TIME_UUID,         null);
        keyspace.createColumnFamily(CF_USER_UNIQUE_UUID,  null);
        keyspace.createColumnFamily(CF_EMAIL_UNIQUE_UUID, null);
        keyspace.createColumnFamily(CF_USERS, ImmutableMap.<String, Object>builder()
                .put("default_validation_class", "UTF8Type")
                .put("column_metadata", ImmutableMap.<String, Object>builder()
                        .put("firstname",  ImmutableMap.<String, Object>builder()
                                .put("validation_class", "UTF8Type")
                                .put("index_name",       "firstname")
                                .put("index_type",       "KEYS")
                                .build())
                        .put("lastname", ImmutableMap.<String, Object>builder()
                                .put("validation_class", "UTF8Type")
                                .put("index_name",       "lastname")
                                .put("index_type",       "KEYS")
                                .build())
                        .put("age", ImmutableMap.<String, Object>builder()
                                .put("validation_class", "LongType")
                                .put("index_name",       "age")
                                .put("index_type",       "KEYS")
                                .build())
                        .build())
                     .build());
        
        keyspace.createColumnFamily(LOCK_CF_LONG, ImmutableMap.<String, Object>builder()
                .put("default_validation_class", "LongType")
                .put("key_validation_class",     "UTF8Type")
                .put("comparator_type",          "UTF8Type")
                .build());
        
        keyspace.createColumnFamily(LOCK_CF_STRING, ImmutableMap.<String, Object>builder()
                .put("default_validation_class", "UTF8Type")
                .put("key_validation_class",     "UTF8Type")
                .put("comparator_type",          "UTF8Type")
                .build());
        ;

        KeyspaceDefinition ki = keyspaceContext.getEntity().describeKeyspace();
        System.out.println("Describe Keyspace: " + ki.getName());

        try {
            //
            // CF_Super :
            // 'A' :
            // 'a' :
            // 1 : 'Aa1',
            // 2 : 'Aa2',
            // 'b' :
            // ...
            // 'z' :
            // ...
            // 'B' :
            // ...
            //
            // CF_Standard :
            // 'A' :
            // 'a' : 1,
            // 'b' : 2,
            // ...
            // 'z' : 26,
            // 'B' :
            // ...
            //

            MutationBatch m;
            OperationResult<Void> result;
            m = keyspace.prepareMutationBatch();

            for (char keyName = 'A'; keyName <= 'Z'; keyName++) {
                String rowKey = Character.toString(keyName);
                ColumnListMutation<String> cfmStandard = m.withRow(
                        CF_STANDARD1, rowKey);
                for (char cName = 'a'; cName <= 'z'; cName++) {
                    cfmStandard.putColumn(Character.toString(cName),
                            (int) (cName - 'a') + 1, null);
                }
                cfmStandard
                        .putColumn("Index1", (int) (keyName - 'A') + 1, null);
                cfmStandard.putColumn("Index2", 42, null);
                m.execute();
            }

            m.withRow(CF_STANDARD1, "Prefixes").putColumn("Prefix1_a", 1, null)
                    .putColumn("Prefix1_b", 2, null)
                    .putColumn("prefix2_a", 3, null);

            result = m.execute();

            String rowKey = "A";
            ColumnListMutation<Long> cfmLong = m.withRow(CF_LONGCOLUMN, rowKey);
            for (Long l = -10L; l < 10L; l++) {
                cfmLong.putEmptyColumn(l, null);
            }
            cfmLong.putEmptyColumn(Long.MAX_VALUE, null);
            result = m.execute();

            m.withRow(CF_USER_INFO, "acct1234")
                .putColumn("firstname", "john", null)
                .putColumn("lastname", "smith", null)
                .putColumn("address", "555 Elm St", null)
                .putColumn("age", 30, null)
                .putEmptyColumn("empty");

            m.execute();

        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testHasValue() throws Exception {
        ColumnList<String> response = keyspace.prepareQuery(CF_USER_INFO).getRow("acct1234").execute().getResult();
        Assert.assertEquals("firstname", response.getColumnByName("firstname").getName());
        Assert.assertEquals("firstname", response.getColumnByName("firstname").getName());
        Assert.assertEquals("john", response.getColumnByName("firstname").getStringValue());
        Assert.assertEquals("john", response.getColumnByName("firstname").getStringValue());
        Assert.assertEquals(true,  response.getColumnByName("firstname").hasValue());
        Assert.assertEquals(false, response.getColumnByName("empty").hasValue());
        
    }
    
    @Test
    public void getKeyspaceDefinition() throws Exception {
        KeyspaceDefinition def = keyspaceContext.getEntity().describeKeyspace();
        Collection<String> fieldNames = def.getFieldNames();
        LOG.info("Getting field names");
        for (String field : fieldNames) {
            LOG.info(field);
        }
        LOG.info(fieldNames.toString());
        
        System.out.println(fieldNames.toString());
        
        for (FieldMetadata field : def.getFieldsMetadata()) {
            System.out.println(field.getName() + " = " + def.getFieldValue(field.getName()) + " (" + field.getType() + ")");
        }
        
        for (ColumnFamilyDefinition cfDef : def.getColumnFamilyList()) {
            LOG.info("----------" );
            for (FieldMetadata field : cfDef.getFieldsMetadata()) {
                LOG.info(field.getName() + " = " + cfDef.getFieldValue(field.getName()) + " (" + field.getType() + ")");
            }
        }
    }
    
    @Test
    public void testDescribeRing() throws Exception {
        // [TokenRangeImpl [startToken=0, endToken=0, endpoints=[127.0.0.1]]]
        List<TokenRange> ring = keyspaceContext.getEntity().describeRing();
        LOG.info(ring.toString());
        
        // 127.0.0.1
        RingDescribeHostSupplier ringSupplier = new RingDescribeHostSupplier(keyspaceContext.getEntity(), 9160);
        List<Host> hosts = ringSupplier.get();
        Assert.assertEquals(1, hosts.get(0).getTokenRanges().size());
        LOG.info(hosts.toString());
        
        Supplier<List<Host>> sourceSupplier1 = Suppliers.ofInstance((List<Host>)Lists.newArrayList(new Host("127.0.0.1", 9160)));
        Supplier<List<Host>> sourceSupplier2 = Suppliers.ofInstance((List<Host>)Lists.newArrayList(new Host("127.0.0.2", 9160)));
        
        // 127.0.0.1
        LOG.info(sourceSupplier1.get().toString());
        
        // 127.0.0.2
        LOG.info(sourceSupplier2.get().toString());
        
        hosts = new FilteringHostSupplier(ringSupplier, sourceSupplier1).get();
        LOG.info(hosts.toString());
        
        Assert.assertEquals(1, hosts.size());
        Assert.assertEquals(1, hosts.get(0).getTokenRanges().size());
        hosts = new FilteringHostSupplier(ringSupplier, sourceSupplier2).get();
        LOG.info(hosts.toString());
        Assert.assertEquals(0, hosts.size());
    }
    
    @Test
    public void paginateColumns() {
        String column = "";
        ColumnList<String> columns;
        int pageize = 10;
        try {
            RowQuery<String, String> query = keyspace
                    .prepareQuery(CF_STANDARD1)
                    .getKey("A")
                    .autoPaginate(true)
                    .withColumnRange(
                            new RangeBuilder().setStart(column)
                                    .setLimit(pageize).build());

            while (!(columns = query.execute().getResult()).isEmpty()) {
                for (Column<String> c : columns) {
                }
                // column = Iterables.getLast(columns).getName() + "\u0000";
            }
        } catch (ConnectionException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void example() {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forCluster(TEST_CLUSTER_NAME)
                .forKeyspace(TEST_KEYSPACE_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl("MyConnectionPool")
                                .setMaxConnsPerHost(1).setSeeds(
                                        "127.0.0.1:9160"))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        Keyspace keyspace = context.getEntity();

        MutationBatch m = keyspace.prepareMutationBatch();

        // m.withRow(CF_USER_STATS, "acct1234")
        // .incrementCounterColumn("loginCount", 1);

        try {
            OperationResult<Void> result = m.execute();
        } catch (ConnectionException e) {
            System.out.println(e);
        }

        try {
            OperationResult<ColumnList<String>> result = keyspace
                    .prepareQuery(CF_USER_INFO).getKey("acct1234").execute();
            ColumnList<String> columns = result.getResult();

            // Lookup columns in response by name
            int age = columns.getColumnByName("age").getIntegerValue();
            String address = columns.getColumnByName("address")
                    .getStringValue();

            // Or, iterate through the columns
            for (Column<String> c : result.getResult()) {
                System.out.println(c.getName());
            }
        } catch (ConnectionException e) {
            System.out.println(e);
        }

    }
    
    @Test
    public void testMultiRowUniqueness() {
        DedicatedMultiRowUniquenessConstraint<UUID> constraint = new DedicatedMultiRowUniquenessConstraint<UUID>
                  (keyspace, TimeUUIDUtils.getUniqueTimeUUIDinMicros())
                  .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                  .withRow(CF_USER_UNIQUE_UUID, "user1")
                  .withRow(CF_EMAIL_UNIQUE_UUID, "user1@domain.com");
        
        DedicatedMultiRowUniquenessConstraint<UUID> constraint2 = new DedicatedMultiRowUniquenessConstraint<UUID>
                  (keyspace, TimeUUIDUtils.getUniqueTimeUUIDinMicros())
                  .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                  .withRow(CF_USER_UNIQUE_UUID, "user1")
                  .withRow(CF_EMAIL_UNIQUE_UUID, "user1@domain.com");
        
        try {
            Column<UUID> c = constraint.getUniqueColumn();
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info(e.getMessage());
        }
        
        try {
            constraint.acquire();
            
            Column<UUID> c = constraint.getUniqueColumn();
            LOG.info("Unique column is " + c.getName());
            
            try {
                constraint2.acquire();
                Assert.fail("Should already be acquired");
            }
            catch (NotUniqueException e) {
                
            }
            catch (Exception e) {
                e.printStackTrace();
                Assert.fail();
            }
            finally {
                try {
                    constraint2.release();
                }
                catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        finally {
            try {
                constraint.release();
            }
            catch (Exception e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
        
        try {
            constraint2.acquire();
            Column<UUID> c = constraint.getUniqueColumn();
            LOG.info("Unique column is " + c.getName());
        }
        catch (NotUniqueException e) {
            Assert.fail("Should already be unique");
        }
        catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        finally {
            try {
                constraint2.release();
            }
            catch (Exception e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
        
    }

    @Test
    public void paginateLongColumns() {
        Long column = Long.MIN_VALUE;
        ColumnList<Long> columns;
        int pageize = 10;
        try {
            RowQuery<String, Long> query = keyspace
                    .prepareQuery(CF_LONGCOLUMN)
                    .getKey("A")
                    .autoPaginate(true)
                    .withColumnRange(
                            new RangeBuilder().setStart(column)
                                    .setLimit(pageize).build());

            while (!(columns = query.execute().getResult()).isEmpty()) {
                LOG.info("-----");
                for (Column<Long> c : columns) {
                    LOG.info(Long.toString(c.getName()));
                }
                // column = Iterables.getLast(columns).getName() + "\u0000";
            }
        } catch (ConnectionException e) {
        }
    }

    @Test
    public void getAll() {
        try {
            OperationResult<Rows<String, String>> rows = keyspace
                    .prepareQuery(CF_STANDARD1).getAllRows().setRowLimit(10)
                    .withColumnRange(new RangeBuilder().setLimit(0).build())
                    .setExceptionCallback(new ExceptionCallback() {
                        @Override
                        public boolean onException(ConnectionException e) {
                            Assert.fail(e.getMessage());
                            return true;
                        }
                    }).execute();
            for (Row<String, String> row : rows.getResult()) {
                LOG.info("ROW: " + row.getKey() + " " + row.getColumns().size());
            }
        } catch (ConnectionException e) {
            Assert.fail();
        }
    }
    
    @Test
    public void testAllRowsReader() throws Exception {
        final AtomicLong counter = new AtomicLong(0);
        
        boolean result = new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
                .forEachRow(new Function<Row<String, String>, Boolean>() {
                    @Override
                    public Boolean apply(@Nullable Row<String, String> row) {
                        counter.incrementAndGet();
                        LOG.info("Got a row: " + row.getKey().toString());
                        return true;
                    }
                })
                .build()
                .call();
        
        Assert.assertTrue(result);
        Assert.assertEquals(28, counter.get());
    }
    
    @Test
    public void testAllRowsReaderWithCancel() throws Exception {
        final AtomicLong counter = new AtomicLong(0);
        
        AllRowsReader<String, String> reader = new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
                .withPageSize(3)
                .forEachRow(new Function<Row<String, String>, Boolean>() {
                    @Override
                    public Boolean apply(@Nullable Row<String, String> row) {
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        counter.incrementAndGet();
                        LOG.info("Got a row: " + row.getKey().toString());
                        return true;
                    }
                })
                .build();
        
        
        Future<Boolean> future = Executors.newSingleThreadExecutor().submit(reader);        
        
        Thread.sleep(1000);
        
        reader.cancel();
        
        try {
            boolean result = future.get();
            Assert.assertEquals(false, result);
            Assert.fail();
        }
        catch (Exception e) {
            LOG.info("Failed to execute", e);
        }
        LOG.info("Before: " + counter.get());
        Assert.assertNotSame(28, counter.get());
        Thread.sleep(2000);
        LOG.info("After: " + counter.get());
        Assert.assertNotSame(28, counter.get());
    }

    @Test
    public void testAllRowsReaderConcurrency() throws Exception {
        final AtomicLong counter = new AtomicLong(0);
        
        boolean result = new AllRowsReader.Builder<String, String>(keyspace, CF_STANDARD1)
                .withConcurrencyLevel(4)
                .forEachRow(new Function<Row<String, String>, Boolean>() {
                    @Override
                    public Boolean apply(@Nullable Row<String, String> row) {
                        counter.incrementAndGet();
                        LOG.info("Got a row: " + row.getKey().toString());
                        return true;
                    }
                })
                .build()
                .call();
        
        Assert.assertTrue(result);
        Assert.assertEquals(28, counter.get());
    }

    @Test
    public void getAllWithCallback() {
        try {
            final AtomicLong counter = new AtomicLong();

            keyspace.prepareQuery(CF_STANDARD1).getAllRows().setRowLimit(3)
                    .setRepeatLastToken(false)
                    .withColumnRange(new RangeBuilder().setLimit(2).build())
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
        } catch (ConnectionException e) {
            Assert.fail();
        }
    }

    static class UserInfo implements Serializable {
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
    public void testSerializedClassValue() {
        UserInfo smo = new UserInfo();
        smo.setLastName("Landau");
        smo.setFirstName("Eran");

        try {
            ByteBuffer bb = ObjectSerializer.get().toByteBuffer(smo);
            keyspace.prepareColumnMutation(CF_STANDARD1, "Key_SerializeTest",
                    "Column1").putValue(bb, null).execute();

            UserInfo smo2 = (UserInfo) keyspace.prepareQuery(CF_STANDARD1)
                    .getKey("Key_SerializeTest").getColumn("Column1").execute()
                    .getResult().getValue(ObjectSerializer.get());

            Assert.assertEquals(smo, smo2);
        } catch (ConnectionException e) {
            Assert.fail();
        }
    }    
    
    @Test
    public void testSingleOps() {
        String key = "SingleOpsTest";

        // Set a string value
        try {
            String column = "StringColumn";
            String value = "Theophiles Thistle, the successful thistle-sifter, in sifting a sieve full of un-sifted thistles, thrust three thousand thistles through the thick of his thumb";

            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .putValue(value, null).execute();

            // Read
            String v = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult().getStringValue();
            Assert.assertEquals(v, value);

            // Delete
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .deleteColumn().execute();

            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult()
                        .getStringValue();
                Assert.fail();
            } catch (NotFoundException e) {
            } catch (ConnectionException e) {
                Assert.fail();
            }
        } catch (ConnectionException e) {
            Assert.fail();
        }

        // Set a int value
        try {
            String column = "IntColumn";
            int value = Integer.MAX_VALUE / 4;

            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .putValue(value, null).execute();

            // Read
            int v = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult().getIntegerValue();
            Assert.assertEquals(v, value);

            long vl = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult().getLongValue();
            Assert.assertEquals(vl, value);

            // Delete
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .deleteColumn().execute();

            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult()
                        .getIntegerValue();
                Assert.fail();
            } catch (NotFoundException e) {
            } catch (ConnectionException e) {
                Assert.fail();
            }
        } catch (ConnectionException e) {
            Assert.fail();
        }

        // Set a double value
        try {
            String column = "IntColumn";
            double value = 3.14;

            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .putValue(value, null).execute();

            // Read
            double v = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult().getDoubleValue();
            Assert.assertEquals(v, value);

            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult()
                        .getIntegerValue();
                Assert.fail();
            } catch (Exception e) {
            }

            // Delete
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .deleteColumn().execute();

            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult()
                        .getDoubleValue();
                Assert.fail();
            } catch (NotFoundException e) {
            } catch (ConnectionException e) {
                Assert.fail();
            }
        } catch (ConnectionException e) {
            Assert.fail();
        }

        // Set a long value
        try {
            String column = "IntColumn";
            long value = Long.MAX_VALUE / 4;

            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .putValue(value, null).execute();

            // Read
            long v = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult().getLongValue();
            Assert.assertEquals(v, value);

            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult()
                        .getIntegerValue();
                Assert.fail();
            } catch (Exception e) {
            }

            // Delete
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .deleteColumn().execute();

            try {
                keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                        .getColumn(column).execute().getResult().getLongValue();
                Assert.fail();
            } catch (NotFoundException e) {
            } catch (ConnectionException e) {
                Assert.fail();
            }
        } catch (ConnectionException e) {
            Assert.fail();
        }
        
        // Set a long value
        try {
            String column = "TimestampColumn";
            long value = Long.MAX_VALUE / 4;

            // Set
            keyspace.prepareColumnMutation(CF_STANDARD1, key, column)
                    .withTimestamp(100)
                    .putValue(value, null)
                    .execute();

            // Read
            Column<String> c = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column).execute().getResult();
            Assert.assertEquals(100,  c.getTimestamp());
        } catch (ConnectionException e) {
            Assert.fail();
        }
    }

    @Test
    public void testTimeUUIDUnique() {
        long now = System.currentTimeMillis();
        UUID uuid1 = TimeUUIDUtils.getTimeUUID(now);
        UUID uuid2 = TimeUUIDUtils.getTimeUUID(now);
        LOG.info(uuid1.toString());
        LOG.info(uuid2.toString());
        Assert.assertTrue(uuid1.equals(uuid2));
    }

    @Test
    public void testTimeUUID2() {
        MutationBatch m = keyspace.prepareMutationBatch();
        String rowKey = "Key2";
        m.withRow(CF_TIME_UUID, rowKey).delete();
        try {
            m.execute();
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        long now = System.currentTimeMillis();
        long msecPerDay = 86400000;
        for (int i = 0; i < 100; i++) {
            m.withRow(CF_TIME_UUID, rowKey).putColumn(
                    TimeUUIDUtils.getTimeUUID(now - i * msecPerDay), i, null);
        }
        try {
            m.execute();
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        try {
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
            for (Column<UUID> column : result.getResult()) {
                System.out.println((now - TimeUUIDUtils.getTimeFromUUID(column
                        .getName())) / msecPerDay);
            }
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testTimeUUID() {
        MutationBatch m = keyspace.prepareMutationBatch();

        UUID columnName = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        long columnTime = TimeUUIDUtils.getTimeFromUUID(columnName);
        String rowKey = "Key1";

        m.withRow(CF_TIME_UUID, rowKey).delete();
        try {
            m.execute();
        } catch (ConnectionException e1) {
            Assert.fail();
        }

        int startTime = 100;
        int endTime = 200;

        m.withRow(CF_TIME_UUID, rowKey).putColumn(columnName, 42, null);
        for (int i = startTime; i < endTime; i++) {
            // UUID c = TimeUUIDUtils.getTimeUUID(i);
            LOG.info(TimeUUIDUtils.getTimeUUID(columnTime + i).toString());

            m.withRow(CF_TIME_UUID, rowKey).putColumn(
                    TimeUUIDUtils.getTimeUUID(columnTime + i), i, null);
        }

        try {
            m.execute();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        try {
            OperationResult<Column<UUID>> result = keyspace
                    .prepareQuery(CF_TIME_UUID).getKey(rowKey)
                    .getColumn(columnName).execute();

            Assert.assertEquals(columnName, result.getResult().getName());
            Assert.assertTrue(result.getResult().getIntegerValue() == 42);

            OperationResult<ColumnList<UUID>> result2 = keyspace
                    .prepareQuery(CF_TIME_UUID).getKey(rowKey).execute();

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

        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        UUID currentUUID = TimeUUIDUtils.getUniqueTimeUUIDinMicros();

        SerializerPackage pkg = null;
        try {
            pkg = keyspace.getSerializerPackage(CF_TIME_UUID.getName(), false);
        } catch (ConnectionException e) {
            Assert.fail();
            e.printStackTrace();
        } catch (UnknownComparatorException e) {
            Assert.fail();
            e.printStackTrace();
        }
        Serializer<UUID> serializer = (Serializer<UUID>) pkg
                .getColumnNameSerializer();

        ByteBuffer buffer = serializer.toByteBuffer(currentUUID);
        String value = serializer.getString(buffer);
        LOG.info("UUID Time = " + value);

        // Test timeUUID pagination
        RowQuery<String, UUID> query = keyspace
                .prepareQuery(CF_TIME_UUID)
                .getKey(rowKey)
                .withColumnRange(
                        new RangeBuilder()
                                .setLimit(10)
                                .setStart(
                                        TimeUUIDUtils.getTimeUUID(columnTime
                                                + startTime))
                                .setEnd(TimeUUIDUtils.getTimeUUID(columnTime
                                        + endTime)).build()).autoPaginate(true);
        OperationResult<ColumnList<UUID>> result;
        int pageCount = 0;
        int rowCount = 0;
        try {
            LOG.info("starting pagination");
            while (!(result = query.execute()).getResult().isEmpty()) {
                pageCount++;
                rowCount += result.getResult().size();
                LOG.info("==== Block ====");
                for (Column<UUID> column : result.getResult()) {
                    LOG.info("Column is " + column.getName());
                }
            }
            LOG.info("pagination complete");
        } catch (ConnectionException e) {
            Assert.fail();
            LOG.info(e.getMessage());
            e.printStackTrace();
        }

    }

    @Test
    public void testCopy() {
        String keyName = "A";

        try {
            keyspace.prepareQuery(CF_STANDARD1).getKey(keyName)
                    .copyTo(CF_STANDARD2, keyName).execute();

            ColumnList<String> list1 = keyspace.prepareQuery(CF_STANDARD1)
                    .getKey(keyName).execute().getResult();

            ColumnList<String> list2 = keyspace.prepareQuery(CF_STANDARD2)
                    .getKey(keyName).execute().getResult();

            Iterator<Column<String>> iter1 = list1.iterator();
            Iterator<Column<String>> iter2 = list2.iterator();

            while (iter1.hasNext()) {
                Column<String> column1 = iter1.next();
                Column<String> column2 = iter2.next();
                Assert.assertEquals(column1.getName(), column2.getName());
                Assert.assertEquals(column1.getByteBufferValue(),
                        column2.getByteBufferValue());
            }
            Assert.assertFalse(iter2.hasNext());

        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMutationBatchMultipleWithRow() throws Exception {
        MutationBatch mb = keyspace.prepareMutationBatch();
        
        Long key = 9L;
        
        mb.withRow(CF_USERS, key).delete();
        mb.withRow(CF_USERS, key).putEmptyColumn("test", null);
        
        mb.execute();
        
        ColumnList<String> result = keyspace.prepareQuery(CF_USERS).getRow(key).execute().getResult();
        
        Assert.assertEquals(1, result.size());
    }
    
    @Test
    public void testClickStream() {
        MutationBatch m = keyspace.prepareMutationBatch();
        String userId = "UserId";

        long timeCounter = 0;
        for (int i = 0; i < 10; i++) {
            String sessionId = "Session" + i;

            for (int j = 0; j < 10; j++) {
                m.withRow(CF_CLICK_STREAM, userId).putColumn(
                        new SessionEvent(sessionId,
                                TimeUUIDUtils.getTimeUUID(j)),
                        Long.toString(timeCounter), null);
                timeCounter++;
            }
        }

        try {
            m.execute();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        try {
            OperationResult<ColumnList<SessionEvent>> result;

            result = keyspace
                    .prepareQuery(CF_CLICK_STREAM)
                    .getKey(userId)
                    .withColumnRange(
                            SE_SERIALIZER.buildRange()
                                    .greaterThanEquals("Session3")
                                    .lessThanEquals("Session5").build())
                    .execute();
            // Assert.assertEquals(10, result.getResult().size());
            // LOG.info("*********************** INCLUSIVE - INCLUSIVE");
            // for (Column<SessionEvent> column : result.getResult()) {
            // LOG.info("####### " + column.getName() + " = " +
            // column.getLongValue());
            // }

            result = keyspace
                    .prepareQuery(CF_CLICK_STREAM)
                    .getKey(userId)
                    .withColumnRange(
                            SE_SERIALIZER.buildRange()
                                    .greaterThanEquals("Session3")
                                    .lessThan("Session5").build()).execute();
            // Assert.assertEquals(10, result.getResult().size());
            // LOG.info("XXXXXXXXXXXXXXXXXXXXXXXX INCLUSIVE - NON_INCLUSIVE");
            // for (Column<SessionEvent> column : result.getResult()) {
            // LOG.info("####### " + column.getName() + " = " +
            // column.getLongValue());
            // }

            result = keyspace
                    .prepareQuery(CF_CLICK_STREAM)
                    .getKey(userId)
                    .withColumnRange(
                            SE_SERIALIZER.buildRange().greaterThan("Session3")
                                    .lessThanEquals("Session5").build())
                    .execute();
            // LOG.info("XXXXXXXXXXXXXXXXXXXXXXXX NON_INCLUSIVE - INCLUSIVE");
            // Assert.assertEquals(10, result.getResult().size());
            // for (Column<SessionEvent> column : result.getResult()) {
            // LOG.info("####### " + column.getName() + " = " +
            // column.getLongValue());
            // }

            result = keyspace
                    .prepareQuery(CF_CLICK_STREAM)
                    .getKey(userId)
                    .withColumnRange(
                            SE_SERIALIZER.buildRange().greaterThan("Session3")
                                    .lessThan("Session5").build()).execute();
            // LOG.info("XXXXXXXXXXXXXXXXXXXXXXXX NON_INCLUSIVE - NON_INCLUSIVE");
            // for (Column<SessionEvent> column : result.getResult()) {
            // LOG.info("####### " + column.getName() + " = " +
            // column.getLongValue());
            // }

            result = keyspace
                    .prepareQuery(CF_CLICK_STREAM)
                    .getKey(userId)
                    .withColumnRange(
                            SE_SERIALIZER
                                    .buildRange()
                                    .withPrefix("Session3")
                                    .greaterThanEquals(
                                            TimeUUIDUtils.getTimeUUID(2))
                                    .lessThanEquals(
                                            TimeUUIDUtils.getTimeUUID(8))
                                    .build()).execute();

            // Assert.assertEquals(10, result.getResult().size());
            // LOG.info("XXXXXXXXXXXXXXXXXXXXXXXX EQUAL - EQUAL");
            // for (Column<SessionEvent> column : result.getResult()) {
            // LOG.info("####### " + column.getName() + " = " +
            // column.getLongValue());
            // }
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Test
    public void testChangeConsistencyLevel() {
        try {
            keyspace.prepareQuery(CF_STANDARD1)
                    .setConsistencyLevel(ConsistencyLevel.CL_ONE).getKey("A")
                    .execute();
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }
    }

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

    @Test
    public void testComposite() {
        String rowKey = "Composite1";

        boolean bool = false;
        MutationBatch m = keyspace.prepareMutationBatch();
        ColumnListMutation<MockCompositeType> mRow = m.withRow(CF_COMPOSITE,
                rowKey);
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
        LOG.info("Created " + columnCount + " columns");
        
        try {
            m.execute();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        OperationResult<ColumnList<MockCompositeType>> result;
        try {
            result = keyspace.prepareQuery(CF_COMPOSITE).getKey(rowKey)
                    .execute();
            Assert.assertEquals(columnCount,  result.getResult().size());
            for (Column<MockCompositeType> col : result.getResult()) {
                LOG.info("COLUMN: " + col.getName().toString());
            }
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        try {
            Column<MockCompositeType> column = keyspace
                    .prepareQuery(CF_COMPOSITE).getKey(rowKey)
                    .getColumn(new MockCompositeType("a", 0, 10, true, "UTF"))
                    .execute().getResult();
            LOG.info("Got single column: " + column.getName().toString());
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        LOG.info("Range builder");
        try {
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
            for (Column<MockCompositeType> col : result.getResult()) {
                LOG.info("COLUMN: " + col.getName().toString());
            }
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        
        /*
         * Composite c = new Composite(); c.addComponent("String1",
         * StringSerializer.get()) .addComponent(123, IntegerSerializer.get());
         * 
         * MutationBatch m = keyspace.prepareMutationBatch();
         * m.withRow(CF_COMPOSITE, "Key1") .putColumn(c, 123, null);
         * 
         * try { m.execute(); } catch (ConnectionException e) { Assert.fail(); }
         * 
         * try { OperationResult<Column<Composite>> result =
         * keyspace.prepareQuery(CF_COMPOSITE) .getKey("Key1") .getColumn(c)
         * .execute();
         * 
         * Assert.assertEquals(123, result.getResult().getIntegerValue()); }
         * catch (ConnectionException e) { Assert.fail(); }
         */
    }

    @Test
    public void testCompositeSlice() throws ConnectionException {
        AnnotatedCompositeSerializer<MockCompositeType> ser = new AnnotatedCompositeSerializer<MockCompositeType>(
                MockCompositeType.class);

        keyspace.prepareQuery(CF_COMPOSITE)
                .getKey("Key1")
                .withColumnRange(
                        ser.makeEndpoint("sessionid1", Equality.LESS_THAN)
                                .toBytes(),
                        ser.makeEndpoint("sessionid1", Equality.GREATER_THAN)
                                .toBytes(), false, 100).execute();
    }

    @Test
    public void testIndexQueryWithPagination() {
        OperationResult<Rows<String, String>> result;
        try {
            LOG.info("************************************************** testIndexQueryWithPagination: ");

            int rowCount = 0;
            int pageCount = 0;
            IndexQuery<String, String> query = keyspace
                    .prepareQuery(CF_STANDARD1).searchWithIndex()
                    .setRowLimit(10).autoPaginateRows(true).addExpression()
                    .whereColumn("Index2").equals().value(42);

            while (!(result = query.execute()).getResult().isEmpty()) {
                pageCount++;
                rowCount += result.getResult().size();
                LOG.info("==== Block ====");
                for (Row<String, String> row : result.getResult()) {
                    LOG.info("RowKey is " + row.getKey());
                }
            }

            Assert.assertEquals(pageCount, 3);
            Assert.assertEquals(rowCount, 26);
            LOG.info("************************************************** Index query: "
                    + result.getResult().size());

        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            e.printStackTrace();
            Assert.fail();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testIndexQuery() {
        OperationResult<Rows<String, String>> result;
        try {
            LOG.info("************************************************** prepareGetMultiRowIndexQuery: ");

            result = keyspace.prepareQuery(CF_STANDARD1).searchWithIndex()
                    .setStartKey("").addExpression().whereColumn("Index1")
                    .equals().value(26).execute();
            Assert.assertEquals(1, result.getResult().size());
            Assert.assertEquals("Z", result.getResult().getRowByIndex(0)
                    .getKey());
            /*
             * for (Row<String, String> row : result.getResult()) {
             * LOG.info("RowKey is " + row.getKey()); for (Column<String> column
             * : row.getColumns()) { LOG.info("  Column: " + column.getName() +
             * "=" + column.getIntegerValue()); } }
             */

            LOG.info("************************************************** Index query: "
                    + result.getResult().size());

        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            e.printStackTrace();
            Assert.fail();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testPreparedIndexQuery() {
        OperationResult<Rows<String, String>> result;
        try {
            LOG.info("************************************************** prepareGetMultiRowIndexQuery: ");

            PreparedIndexExpression<String, String> clause = CF_STANDARD1
                    .newIndexClause().whereColumn("Index1").equals().value(26);

            result = keyspace.prepareQuery(CF_STANDARD1).searchWithIndex()
                    .setStartKey("")
                    .addPreparedExpressions(Arrays.asList(clause)).execute();

            for (Row<String, String> row : result.getResult()) {
                LOG.info("RowKey is " + row.getKey() + " columnCount="
                        + row.getColumns().size());
                for (Column<String> column : row.getColumns()) {
                    LOG.info("  Column: " + column.getName() + "="
                            + column.getIntegerValue());
                }
            }
            Assert.assertEquals(1, result.getResult().size());
            Assert.assertEquals("Z", result.getResult().getRowByIndex(0)
                    .getKey());

            LOG.info("************************************************** Index query: "
                    + result.getResult().size());

        } catch (ConnectionException e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Test
    public void testIncrementCounter() {
        long baseAmount, incrAmount = 100;
        Column<String> column;

        try {
            column = getColumnValue(CF_COUNTER1, "CounterRow1", "MyCounter");
            baseAmount = column.getLongValue();
        } catch (Exception e) {
            baseAmount = 0;
        }

        MutationBatch m = keyspace.prepareMutationBatch();
        m.withRow(CF_COUNTER1, "CounterRow1").incrementCounterColumn(
                "MyCounter", incrAmount);
        try {
            m.execute();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        column = getColumnValue(CF_COUNTER1, "CounterRow1", "MyCounter");
        Assert.assertNotNull(column);
        Assert.assertEquals(column.getLongValue(), baseAmount + incrAmount);

        m = keyspace.prepareMutationBatch();
        m.withRow(CF_COUNTER1, "CounterRow1").incrementCounterColumn(
                "MyCounter", incrAmount);
        try {
            m.execute();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        column = getColumnValue(CF_COUNTER1, "CounterRow1", "MyCounter");
        Assert.assertNotNull(column);
        Assert.assertEquals(column.getLongValue(), baseAmount + 2 * incrAmount);
    }

    @Test
    public void testDeleteCounter() {
        Column<String> column;
        String rowKey = "CounterRowDelete1";
        String counterName = "MyCounter";

        // Increment the column
        MutationBatch m = keyspace.prepareMutationBatch();
        m.withRow(CF_COUNTER1, rowKey).incrementCounterColumn(counterName, 1);
        try {
            m.execute();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        // Read back the value
        column = getColumnValue(CF_COUNTER1, rowKey, counterName);
        Assert.assertNotNull(column);
        Assert.assertEquals(column.getLongValue(), 1);

        // Delete the column
        try {
            // keyspace.prepareColumnMutation(CF_COUNTER1, rowKey, counterName)
            // .deleteCounterColumn().execute();
            keyspace.prepareColumnMutation(CF_COUNTER1, rowKey, counterName)
                    .deleteCounterColumn().execute();
            /*
             * m = keyspace.prepareMutationBatch(); m.withRow(CF_COUNTER1,
             * rowKey) .deleteColumn(counterName); m.execute();
             */
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        // Try to read back
        // This should be non-existent
        column = getColumnValue(CF_COUNTER1, rowKey, counterName);
        if (column != null) {
            LOG.error("Counter has value: " + column.getLongValue());
            Assert.fail();
        }
    }

    @Test
    public void testCql() {
        try {
            System.out.println("testCQL");
            LOG.info("CQL Test");
            OperationResult<CqlResult<String, String>> result = keyspace
                    .prepareQuery(CF_STANDARD1)
                    .withCql("SELECT * FROM Standard1;").execute();
            Assert.assertTrue(result.getResult().hasRows());
            Assert.assertEquals(30, result.getResult().getRows().size());
            Assert.assertFalse(result.getResult().hasNumber());
            
            Row<String, String> row;
            
            row = result.getResult().getRows().getRow("A");
            Assert.assertEquals("A", row.getKey());
            
            row = result.getResult().getRows().getRow("B");
            Assert.assertEquals("B", row.getKey());
            
            row = result.getResult().getRows().getRow("NonExistent");
            Assert.assertNull(row);
            
            row = result.getResult().getRows().getRowByIndex(10);
            Assert.assertEquals("I", row.getKey());
            
            for (Row<String, String> row1 : result.getResult().getRows()) {
              LOG.info("KEY***: " + row1.getKey()); 
            }
            
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Test
    public void testCqlCount() {
        try {
            LOG.info("CQL Test");
            OperationResult<CqlResult<String, String>> result = keyspace
                    .prepareQuery(CF_STANDARD1)
                    .withCql("SELECT count(*) FROM Standard1 where KEY='A';")
                    .execute();

            long count = result.getResult().getRows().getRowByIndex(0).getColumns().getColumnByName("count").getLongValue();
            LOG.info("CQL Count: " + count);
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Test
    public void testGetSingleColumn() {
        Column<String> column = getColumnValue(CF_STANDARD1, "A", "a");
        Assert.assertNotNull(column);
        Assert.assertEquals(1, column.getIntegerValue());
    }

    @Test
    public void testColumnFamilyDoesntExist() {
        ColumnFamily<String, String> cf = new ColumnFamily<String, String>(
                "DoesntExist", StringSerializer.get(), StringSerializer.get());
        OperationResult<Void> result;
        try {
            MutationBatch m = keyspace.prepareMutationBatch();
            m.withRow(cf, "Key1").putColumn("Column2", "Value2", null);
            result = m.execute();
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testKeyspaceDoesntExist() {
        AstyanaxContext<Keyspace> keyspaceContext = new AstyanaxContext.Builder()
                .forCluster(TEST_CLUSTER_NAME)
                .forKeyspace(TEST_KEYSPACE_NAME + "_DOESNT_EXIST")
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME
                                + "_" + TEST_KEYSPACE_NAME + "_DOESNT_EXIST")
                                .setMaxConnsPerHost(1).setSeeds(SEEDS))
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        try {
            keyspaceContext.start();

            Keyspace ks = keyspaceContext.getEntity();

            OperationResult<Void> result = null;
            try {
                MutationBatch m = ks.prepareMutationBatch();
                m.withRow(CF_STANDARD1, "Key1").putColumn("Column2", "Value2",
                        null);
                result = m.execute();
                Assert.fail();
            } catch (ConnectionException e) {
                LOG.info(e.getMessage());
            }
        } finally {
            keyspaceContext.shutdown();
        }
    }

    @Test
    public void testGetSingleColumnNotExists() {
        Column<String> column = getColumnValue(CF_STANDARD1, "A",
                "DoesNotExist");
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
    public void testGetSingleKeyNotExists() {
        Column<String> column = getColumnValue(CF_STANDARD1, "AA", "ab");
        Assert.assertNull(column);
    }

    @Test
    public void testFunctionalQuery() throws ConnectionException {
        OperationResult<ColumnList<String>> r1 = keyspace
                .prepareQuery(CF_STANDARD1).getKey("A").execute();
        Assert.assertEquals(28, r1.getResult().size());

        /*
         * OperationResult<Rows<String, String>> r2 = keyspace.prepareQuery()
         * .fromColumnFamily(CF_STANDARD1) .selectKeyRange("A", "Z", null, null,
         * 5) .execute();
         */
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
    public void testGetColumnsWithPrefix() throws ConnectionException {
        OperationResult<ColumnList<String>> r = keyspace
                .prepareQuery(CF_STANDARD1)
                .getKey("Prefixes")
                .withColumnRange("Prefix1_\u00000", "Prefix1_\uffff", false,
                        Integer.MAX_VALUE).execute();
        Assert.assertEquals(2, r.getResult().size());
        Assert.assertEquals("Prefix1_a", r.getResult().getColumnByIndex(0)
                .getName());
        Assert.assertEquals("Prefix1_b", r.getResult().getColumnByIndex(1)
                .getName());
    }

    @Test
    public void testGetCounters() throws ConnectionException {
        LOG.info("Starting testGetCounters...");

        try {
            OperationResult<Column<String>> result = keyspace
                    .prepareQuery(CF_COUNTER1).getKey("CounterRow1")
                    .getColumn("TestCounter").execute();

            Long count = result.getResult().getLongValue();
            Assert.assertNotNull(count);
            Assert.assertTrue(count > 0);
        } catch (NotFoundException e) {

        }

        LOG.info("... testGetCounters done");
    }

    @Test
    public void testGetSingleKey() {
        try {
            for (char key = 'A'; key <= 'Z'; key++) {
                String keyName = Character.toString(key);
                OperationResult<ColumnList<String>> result = keyspace
                        .prepareQuery(CF_STANDARD1).getKey(keyName).execute();

                Assert.assertNotNull(result.getResult());

                System.out.printf("%s executed on %s in %d msec size=%d\n",
                        keyName, result.getHost(), result.getLatency(), result
                                .getResult().size());
            }
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Test
    public void testGetSingleKeyAsync() {
        try {
            Future<OperationResult<ColumnList<String>>> result = keyspace
                    .prepareQuery(CF_STANDARD1).getKey("A").executeAsync();

            result.get(1000, TimeUnit.MILLISECONDS);

        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            e.printStackTrace();
            Assert.fail();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
            e.printStackTrace();
            Assert.fail();
        } catch (ExecutionException e) {
            LOG.error(e.getMessage(), e);
            e.printStackTrace();
            Assert.fail();
        } catch (TimeoutException e) {
            LOG.error(e.getMessage(), e);
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetAllKeysRoot() {
        LOG.info("Starting testGetAllKeysRoot...");

        try {
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
            Assert.assertEquals("M", row.getKey());
            /*
             * LOG.info("Get " + result.getResult().size() + " keys"); for
             * (Row<String, String> row : result.getResult()) {
             * LOG.info(String.format("%s executed on %s in %d msec size=%d\n",
             * row.getKey(), result.getHost(), result.getLatency(),
             * row.getColumns().size())); for (Column<String> sc :
             * row.getColumns()) { LOG.info("  " + sc.getName());
             * ColumnList<Integer> subColumns =
             * sc.getSubColumns(IntegerSerializer.get()); for (Column<Integer>
             * sub : subColumns) { LOG.info("    " + sub.getName() + "=" +
             * sub.getStringValue()); } } }
             */

        } catch (ConnectionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail();
        }

        LOG.info("... testGetAllKeysRoot");
    }

    @Test
    public void testGetColumnSlice() {
        LOG.info("Starting testGetColumnSlice...");
        try {
            OperationResult<ColumnList<String>> result = keyspace
                    .prepareQuery(CF_STANDARD1)
                    .getKey("A")
                    .withColumnSlice(
                            new ColumnSlice<String>("c", "h").setLimit(5))
                    .execute();
            Assert.assertNotNull(result.getResult());
            Assert.assertEquals(5, result.getResult().size());
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testGetAllKeysPath() {
        LOG.info("Starting testGetAllKeysPath...");

        try {
            List<String> keys = new ArrayList<String>();
            for (char key = 'A'; key <= 'Z'; key++) {
                String keyName = Character.toString(key);
                keys.add(keyName);
            }

            OperationResult<Rows<String, String>> result = keyspace
                    .prepareQuery(CF_STANDARD1)
                    .getKeySlice(keys.toArray(new String[keys.size()]))
                    .execute();
            /*
             * System.out.printf("%s executed on %s in %d msec size=%d\n",
             * row.getKey(), result.getHost(), result.getLatency(),
             * row.getColumns().size());
             */

            // for (Row<String, String> row : result.getResult()) {
            // for (Column<Integer> column : row.getColumns()) {
            // System.out.println("  Column: " + column.getName());
            // }
            // }
            
            OperationResult<Map<String, Integer>> counts = keyspace
                .prepareQuery(CF_STANDARD1)
                .getKeySlice(keys.toArray(new String[keys.size()]))
                .getColumnCounts()
                .execute();
                        
            Assert.assertEquals(26, counts.getResult().size());
            
            for (Entry<String, Integer> count : counts.getResult().entrySet()) {
                Assert.assertEquals(new Integer(28), count.getValue());
            }
            
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        LOG.info("Starting testGetAllKeysPath...");
    }
    
    @Test
    public void testDeleteMultipleKeys() {
        LOG.info("Starting testDeleteMultipleKeys...");
        LOG.info("... testGetAllKeysPath");

    }

    @Test
    public void testMutationMerge() {
        MutationBatch m1 = keyspace.prepareMutationBatch();
        MutationBatch m2 = keyspace.prepareMutationBatch();
        MutationBatch m3 = keyspace.prepareMutationBatch();
        MutationBatch m4 = keyspace.prepareMutationBatch();
        MutationBatch m5 = keyspace.prepareMutationBatch();

        m1.withRow(CF_STANDARD1, "1").putColumn("1", "X", null);
        m2.withRow(CF_STANDARD1, "2").putColumn("2", "X", null)
                .putColumn("3", "X", null);
        m3.withRow(CF_STANDARD1, "3").putColumn("4", "X", null)
                .putColumn("5", "X", null).putColumn("6", "X", null);
        m4.withRow(CF_STANDARD1, "1").putColumn("7", "X", null)
                .putColumn("8", "X", null).putColumn("9", "X", null)
                .putColumn("10", "X", null);

        MutationBatch merged = keyspace.prepareMutationBatch();
        LOG.info(merged.toString());
        Assert.assertEquals(merged.getRowCount(), 0);

        merged.mergeShallow(m1);
        LOG.info(merged.toString());
        Assert.assertEquals(merged.getRowCount(), 1);

        merged.mergeShallow(m2);
        LOG.info(merged.toString());
        Assert.assertEquals(merged.getRowCount(), 2);

        merged.mergeShallow(m3);
        LOG.info(merged.toString());
        Assert.assertEquals(merged.getRowCount(), 3);

        merged.mergeShallow(m4);
        LOG.info(merged.toString());
        Assert.assertEquals(merged.getRowCount(), 3);

        merged.mergeShallow(m5);
        LOG.info(merged.toString());
        Assert.assertEquals(merged.getRowCount(), 3);
    }

    @Test
    public void testDelete() {
        LOG.info("Starting testDelete...");

        String rowKey = "DeleteMe_testDelete";

        MutationBatch m = keyspace.prepareMutationBatch();
        m.withRow(CF_STANDARD1, rowKey).putColumn("Column1", "X", null)
                .putColumn("Column2", "X", null);

        try {
            m.execute();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        Assert.assertEquals(getColumnValue(CF_STANDARD1, rowKey, "Column1")
                .getStringValue(), "X");
        Assert.assertTrue(deleteColumn(CF_STANDARD1, rowKey, "Column1"));
        Assert.assertNull(getColumnValue(CF_STANDARD1, rowKey, "Column1"));

        LOG.info("... testDelete");
    }

    @Test
    public void testDeleteLotsOfColumns() {
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

        try {
            m.execute();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        // Verify count
        try {
            int count = keyspace.prepareQuery(CF_STANDARD1)
                    .setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
                    .getKey(rowKey).getCount().execute().getResult();
            Assert.assertEquals(nColumns, count);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        // Delete half of the columns
        m = keyspace.prepareMutationBatch().setConsistencyLevel(
                ConsistencyLevel.CL_QUORUM);
        rm = m.withRow(CF_STANDARD1, rowKey);

        for (int i = 0; i < nColumns / 2; i++) {
            rm.deleteColumn("" + i);
        }

        try {
            m.execute();
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        // Verify count
        try {
            int count = getRowColumnCount(CF_STANDARD1, rowKey);
            Assert.assertEquals(nColumns / 2, count);

            count = getRowColumnCountWithPagination(CF_STANDARD1, rowKey,
                    pageSize);
            Assert.assertEquals(nColumns / 2, count);

        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        // Delete all of the columns
        m = keyspace.prepareMutationBatch().setConsistencyLevel(
                ConsistencyLevel.CL_QUORUM);
        rm = m.withRow(CF_STANDARD1, rowKey);

        for (int i = 0; i < nColumns; i++) {
            rm.deleteColumn("" + i);
        }

        try {
            m.execute();
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        // Verify count
        try {
            int count = getRowColumnCount(CF_STANDARD1, rowKey);
            Assert.assertEquals(0, count);

            count = getRowColumnCountWithPagination(CF_STANDARD1, rowKey,
                    pageSize);
            Assert.assertEquals(0, count);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        LOG.info("... testDelete");
    }

    private <K, C> int getRowColumnCount(ColumnFamily<K, C> cf, K rowKey)
            throws ConnectionException {
        int count = keyspace.prepareQuery(cf)
                .setConsistencyLevel(ConsistencyLevel.CL_QUORUM).getKey(rowKey)
                .getCount().execute().getResult();

        return count;
    }

    private <K, C> int getRowColumnCountWithPagination(ColumnFamily<K, C> cf,
            K rowKey, int pageSize) throws ConnectionException {
        RowQuery<K, C> query = keyspace.prepareQuery(cf)
                .setConsistencyLevel(ConsistencyLevel.CL_QUORUM).getKey(rowKey)
                .withColumnRange(new RangeBuilder().setLimit(pageSize).build())
                .autoPaginate(true);

        ColumnList<C> result;
        int count = 0;
        while (!(result = query.execute().getResult()).isEmpty()) {
            count += result.size();
        }

        return count;
    }

    @Test
    public void testCsvLoader() {
        StringBuilder sb = new StringBuilder()
                .append("key, firstname, lastname, age, test\n")
                .append("1, eran, landau, 34, a\n")
                .append("2, netta, landau, 33, b\n")
                .append("3, arielle, landau, 6, c\n")
                .append("4, eyal, landau, 2, d\n");

        RecordReader reader = new CsvRecordReader(new StringReader(
                sb.toString()));
        RecordWriter writer = new ColumnarRecordWriter(keyspace,
                CF_USERS.getName());

        try {
            reader.start();
            writer.start();
            List<Pair<String, String>> record = null;
            while (null != (record = reader.next())) {
                writer.write(record);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        } finally {
            reader.shutdown();
            writer.shutdown();
        }

        try {
            Rows<Long, String> rows = keyspace.prepareQuery(CF_USERS)
                    .getAllRows().execute().getResult();
            new JsonRowsWriter(new PrintWriter(System.out, true),
                    keyspace.getSerializerPackage(CF_USERS.getName(), false))
                    .setRowsAsArray(false).write(rows);

            new JsonRowsWriter(new PrintWriter(System.out, true),
                    keyspace.getSerializerPackage(CF_USERS.getName(), false))
                    .setRowsAsArray(true).setCountName("_count_")
                    .setRowsName("_rows_").setNamesName("_names_").write(rows);

            new JsonRowsWriter(new PrintWriter(System.out, true),
                    keyspace.getSerializerPackage(CF_USERS.getName(), false))
                    .setRowsAsArray(true).setDynamicColumnNames(true)
                    .write(rows);

            new JsonRowsWriter(new PrintWriter(System.out, true),
                    keyspace.getSerializerPackage(CF_USERS.getName(), false))
                    .setRowsAsArray(true).setIgnoreUndefinedColumns(true)
                    .write(rows);

            new JsonRowsWriter(new PrintWriter(System.out, true),
                    keyspace.getSerializerPackage(CF_USERS.getName(), false))
                    .setRowsAsArray(true)
                    .setFixedColumnNames("firstname", "lastname")
                    .setIgnoreUndefinedColumns(true).write(rows);

            LOG.info("******* COLUMNS AS ROWS ********");
            new JsonRowsWriter(new PrintWriter(System.out, true),
                    keyspace.getSerializerPackage(CF_USERS.getName(), false))
                    .setRowsAsArray(true).setColumnsAsRows(true).write(rows);

        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Test
    public void testCsvLoaderWithCustomSerializers() {
        StringBuilder sb = new StringBuilder()
                .append("key, firstname, lastname, age, test\n")
                .append("1, eran, landau, 34, a\n")
                .append("2, netta, landau, 33, b\n")
                .append("3, arielle, landau, 6, c\n")
                .append("4, eyal, landau, 2, d\n");

        SerializerPackageImpl pkg = null;
        try {
            pkg = new SerializerPackageImpl().setKeyType("LongType")
                    .setColumnNameType("UTF8Type")
                    .setDefaultValueType("UTF8Type")
                    .setValueType("firstname", "UTF8Type")
                    .setValueType("lastname", "UTF8Type")
                    .setValueType("age", "LongType");
        } catch (UnknownComparatorException e) {
            Assert.fail();
        }

        RecordReader reader = new CsvRecordReader(new StringReader(
                sb.toString()));
        RecordWriter writer = new ColumnarRecordWriter(keyspace,
                CF_USERS.getName(), pkg);

        try {
            reader.start();
            writer.start();
            List<Pair<String, String>> record = null;
            while (null != (record = reader.next())) {
                writer.write(record);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        } finally {
            reader.shutdown();
            writer.shutdown();
        }

        try {
            Rows<Long, String> rows = keyspace.prepareQuery(CF_USERS)
                    .getAllRows().execute().getResult();
            new JsonRowsWriter(new PrintWriter(System.out, true),
                    keyspace.getSerializerPackage(CF_USERS.getName(), false))
                    .setRowsAsArray(false).write(rows);
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Test
    public void testCsvLoaderComposite() {
        StringBuilder sb = new StringBuilder().append("key, column, value\n")
                .append("1, a:1, 1a1\n").append("1, b:1, 2b1\n")
                .append("2, a:1, 3a1\n").append("3, a:1, 4a1\n");

        CsvColumnReader reader = new CsvColumnReader(new StringReader(
                sb.toString()));
        RecordWriter writer = new ColumnarRecordWriter(keyspace,
                CF_COMPOSITE_CSV.getName());

        try {
            reader.start();
            writer.start();
            List<Pair<String, String>> record = null;
            while (null != (record = reader.next())) {
                writer.write(record);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        } finally {
            reader.shutdown();
            writer.shutdown();
        }

        try {
            Rows<ByteBuffer, ByteBuffer> rows = keyspace
                    .prepareQuery(CF_COMPOSITE_CSV).getAllRows().execute()
                    .getResult();
            new JsonRowsWriter(new PrintWriter(System.out, true),
                    keyspace.getSerializerPackage(CF_COMPOSITE_CSV.getName(),
                            false)).setRowsAsArray(false).write(rows);

            new JsonRowsWriter(new PrintWriter(System.out, true),
                    keyspace.getSerializerPackage(CF_COMPOSITE_CSV.getName(),
                            false)).setRowsAsArray(true)
                    .setCountName("_count_").setRowsName("_rows_")
                    .setNamesName("_names_").write(rows);

            new JsonRowsWriter(new PrintWriter(System.out, true),
                    keyspace.getSerializerPackage(CF_COMPOSITE_CSV.getName(),
                            false)).setRowsAsArray(true)
                    .setDynamicColumnNames(true).write(rows);

            new JsonRowsWriter(new PrintWriter(System.out, true),
                    keyspace.getSerializerPackage(CF_COMPOSITE_CSV.getName(),
                            false)).setRowsAsArray(true)
                    .setIgnoreUndefinedColumns(true).write(rows);

            LOG.info("******* COLUMNS AS ROWS ********");
            new JsonRowsWriter(new PrintWriter(System.out, true),
                    keyspace.getSerializerPackage(CF_COMPOSITE_CSV.getName(),
                            false)).setRowsAsArray(true).setColumnsAsRows(true)
                    .write(rows);

        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Test
    public void testCluster() {
        AstyanaxContext<Cluster> clusterContext = new AstyanaxContext.Builder()
                .forCluster(TEST_CLUSTER_NAME)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl())
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME)
                                .setSeeds(SEEDS).setSocketTimeout(30000)
                                .setMaxTimeoutWhenExhausted(200)
                                .setMaxConnsPerHost(1))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildCluster(ThriftFamilyFactory.getInstance());

        clusterContext.start();
        Cluster cluster = clusterContext.getEntity();

        try {
            cluster.describeClusterName();
            List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
            LOG.info("Keyspace count:" + keyspaces.size());
            for (KeyspaceDefinition keyspace : keyspaces) {
                LOG.info("Keyspace: " + keyspace.getName());
            }
            Assert.assertNotNull(keyspaces);
            Assert.assertTrue(keyspaces.size() > 0);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            clusterContext.shutdown();
        }
    }

    @Test
    public void testPrefixedSerializer() {
        ColumnFamily<String, String> cf = new ColumnFamily<String, String>(
                "Standard1", StringSerializer.get(), StringSerializer.get());

        ColumnFamily<String, String> cf1 = new ColumnFamily<String, String>(
                "Standard1", new PrefixedSerializer<String, String>("Prefix1_",
                        StringSerializer.get(), StringSerializer.get()),
                StringSerializer.get());

        ColumnFamily<String, String> cf2 = new ColumnFamily<String, String>(
                "Standard1", new PrefixedSerializer<String, String>("Prefix2_",
                        StringSerializer.get(), StringSerializer.get()),
                StringSerializer.get());

        MutationBatch m = keyspace.prepareMutationBatch();
        m.withRow(cf1, "A").putColumn("Column1", "Value1", null);
        m.withRow(cf2, "A").putColumn("Column1", "Value2", null);

        try {
            m.execute();
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        try {
            OperationResult<ColumnList<String>> result = keyspace
                    .prepareQuery(cf).getKey("Prefix1_A").execute();
            Assert.assertEquals(1, result.getResult().size());
            Column<String> c = result.getResult().getColumnByName("Column1");
            Assert.assertEquals("Value1", c.getStringValue());
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

        try {
            OperationResult<ColumnList<String>> result = keyspace
                    .prepareQuery(cf).getKey("Prefix2_A").execute();
            Assert.assertEquals(1, result.getResult().size());
            Column<String> c = result.getResult().getColumnByName("Column1");
            Assert.assertEquals("Value2", c.getStringValue());
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
        }

    }

    @Test
    public void testWithRetry() {
        String clusterName = TEST_CLUSTER_NAME + "_DOESNT_EXIST";
        AstyanaxContext<Keyspace> keyspaceContext = new AstyanaxContext.Builder()
                .forCluster(clusterName)
                .forKeyspace(TEST_KEYSPACE_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(clusterName + "_"
                                + TEST_KEYSPACE_NAME).setMaxConnsPerHost(1)
                                .setSeeds(SEEDS))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        ColumnFamily<String, String> cf = new ColumnFamily<String, String>(
                "DoesntExist", StringSerializer.get(), StringSerializer.get());
        try {
            MutationBatch m = keyspaceContext.getEntity()
                    .prepareMutationBatch()
                    .withRetryPolicy(new ExponentialBackoff(10, 3));
            m.withRow(cf, "Key1").putColumn("Column2", "Value2", null);
            m.execute();
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.info(e.getMessage());
        }
    }
    
    @Test
    public void testTtl() throws Exception {
        ColumnPrefixDistributedRowLock<String> lock = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_LONG, "testTtl")
                .withTtl(2)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(1,  TimeUnit.SECONDS);
        
        try {
            lock.acquire();
            Assert.assertEquals(1, lock.readLockColumns().size());
            Thread.sleep(3000);
            Assert.assertEquals(0, lock.readLockColumns().size());
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        finally {
            lock.release();
        }    
        Assert.assertEquals(0, lock.readLockColumns().size());
    }
    
    @Test
    public void testTtlString() throws Exception {
        ColumnPrefixDistributedRowLock<String> lock = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_STRING, "testTtl")
                .withTtl(2)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(1,  TimeUnit.SECONDS);
        
        try {
            lock.acquire();
            Assert.assertEquals(1, lock.readLockColumns().size());
            Thread.sleep(3000);
            Assert.assertEquals(0, lock.readLockColumns().size());
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        finally {
            lock.release();
        }    
        Assert.assertEquals(0, lock.readLockColumns().size());
    }
    
    @Test
    public void testStaleLockWithFail() throws Exception {
        ColumnPrefixDistributedRowLock<String> lock1 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_LONG, "testStaleLock")
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(1, TimeUnit.SECONDS);
        
        ColumnPrefixDistributedRowLock<String> lock2 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_LONG, "testStaleLock")
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(9,  TimeUnit.SECONDS);
        
        try {
            lock1.acquire();
            Thread.sleep(5000);
            try {
                lock2.acquire();
            }
            catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            finally {
                lock2.release();
            }
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        finally {
            lock1.release();
        }
    }
    
    @Test
    public void testStaleLockWithFail_String() throws Exception {
        ColumnPrefixDistributedRowLock<String> lock1 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_STRING, "testStaleLock")
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(1, TimeUnit.SECONDS);
        
        ColumnPrefixDistributedRowLock<String> lock2 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_STRING, "testStaleLock")
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(9,  TimeUnit.SECONDS);
        
        try {
            lock1.acquire();
            Thread.sleep(5000);
            try {
                lock2.acquire();
            }
            catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            finally {
                lock2.release();
            }
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        finally {
            lock1.release();
        }
    }
    
    @Test
    public void testStaleLock() throws Exception {
        ColumnPrefixDistributedRowLock<String> lock1 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_LONG, "testStaleLock")
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(1, TimeUnit.SECONDS);
        
        ColumnPrefixDistributedRowLock<String> lock2 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_LONG, "testStaleLock")
                .failOnStaleLock(true)
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(9, TimeUnit.SECONDS);
        
        try {
            lock1.acquire();
            Thread.sleep(2000);
            try {
                lock2.acquire();
                Assert.fail();
            }
            catch (StaleLockException e) {
            }
            catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            finally {
                lock2.release();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        finally {
            lock1.release();
        }
    }
    
    @Test
    public void testStaleLock_String() throws Exception {
        ColumnPrefixDistributedRowLock<String> lock1 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_STRING, "testStaleLock")
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(1, TimeUnit.SECONDS);
        
        ColumnPrefixDistributedRowLock<String> lock2 = 
            new ColumnPrefixDistributedRowLock<String>(keyspace, LOCK_CF_STRING, "testStaleLock")
                .failOnStaleLock(true)
                .withTtl(TTL)
                .withConsistencyLevel(ConsistencyLevel.CL_ONE)
                .expireLockAfter(9, TimeUnit.SECONDS);
        
        try {
            lock1.acquire();
            Thread.sleep(2000);
            try {
                lock2.acquire();
                Assert.fail();
            }
            catch (StaleLockException e) {
            }
            catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            finally {
                lock2.release();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        finally {
            lock1.release();
        }
    }

    private boolean deleteColumn(ColumnFamily<String, String> cf,
            String rowKey, String columnName) {
        MutationBatch m = keyspace.prepareMutationBatch();
        m.withRow(cf, rowKey).deleteColumn(columnName);

        try {
            m.execute();
            return true;
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
            return false;
        }
    }

    private Column<String> getColumnValue(ColumnFamily<String, String> cf,
            String rowKey, String columnName) {
        OperationResult<Column<String>> result;
        try {
            result = keyspace.prepareQuery(cf).getKey(rowKey)
                    .getColumn(columnName).execute();
            return result.getResult();
        } catch (NotFoundException e) {
            LOG.info(e.getMessage());
            return null;
        } catch (ConnectionException e) {
            LOG.error(e.getMessage(), e);
            Assert.fail();
            return null;
        }
    }
}

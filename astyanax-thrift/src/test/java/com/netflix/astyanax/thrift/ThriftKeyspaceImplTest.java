package com.netflix.astyanax.thrift;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
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
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.cql.CqlStatementResult;
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
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.PreparedIndexExpression;
import com.netflix.astyanax.query.RowQuery;
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
import com.netflix.astyanax.test.SessionEvent;
import com.netflix.astyanax.util.ColumnarRecordWriter;
import com.netflix.astyanax.util.CsvColumnReader;
import com.netflix.astyanax.util.CsvRecordReader;
import com.netflix.astyanax.util.JsonRowsWriter;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.RecordReader;
import com.netflix.astyanax.util.RecordWriter;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class ThriftKeyspaceImplTest {

    private static Logger LOG = LoggerFactory.getLogger(ThriftKeyspaceImplTest.class);

    private static Keyspace                  keyspace;
    private static AstyanaxContext<Keyspace> keyspaceContext;

    private static ColumnFamily<String, String> CF_USER_INFO = ColumnFamily.newColumnFamily(
            "UserInfo", // Column Family Name
            StringSerializer.get(), // Key Serializer
            StringSerializer.get()); // Column Serializer

    private static ColumnFamily<Long, Long> CF_DELETE = ColumnFamily
            .newColumnFamily(
                    "delete", 
                    LongSerializer.get(),
                    LongSerializer.get());
    
    private static ColumnFamily<Long, String> CF_USERS = ColumnFamily
            .newColumnFamily(
                    "users", 
                    LongSerializer.get(),
                    StringSerializer.get());

    private static ColumnFamily<String, String> CF_TTL = ColumnFamily
            .newColumnFamily(
                    "ttl", 
                    StringSerializer.get(),
                    StringSerializer.get());

    public static ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
            .newColumnFamily(
                    "Standard1", 
                    StringSerializer.get(),
                    StringSerializer.get());

    public static ColumnFamily<String, Long> CF_LONGCOLUMN = ColumnFamily
            .newColumnFamily(
                    "LongColumn1", 
                    StringSerializer.get(),
                    LongSerializer.get());

    public static ColumnFamily<String, String> CF_STANDARD2 = ColumnFamily
            .newColumnFamily(
                    "Standard2", 
                    StringSerializer.get(),
                    StringSerializer.get());

    public static ColumnFamily<String, String> CF_COUNTER1 = ColumnFamily
            .newColumnFamily(
                    "Counter1", 
                    StringSerializer.get(),
                    StringSerializer.get());

    public static ColumnFamily<String, String> CF_NOT_DEFINED = ColumnFamily
            .newColumnFamily(
                    "NotDefined", 
                    StringSerializer.get(),
                    StringSerializer.get());

    public static ColumnFamily<String, String> CF_EMPTY = ColumnFamily
            .newColumnFamily(
                    "NotDefined", 
                    StringSerializer.get(),
                    StringSerializer.get());

    public static ColumnFamily<Long, Long> ATOMIC_UPDATES = ColumnFamily
            .newColumnFamily(
                    "AtomicUpdates", 
                    LongSerializer.get(),
                    LongSerializer.get());

    public static AnnotatedCompositeSerializer<MockCompositeType> M_SERIALIZER = new AnnotatedCompositeSerializer<MockCompositeType>(
            MockCompositeType.class);
    
    public static ColumnFamily<String, MockCompositeType> CF_COMPOSITE = ColumnFamily
            .newColumnFamily(
                    "CompositeColumn", 
                    StringSerializer.get(),
                    M_SERIALIZER);

    public static ColumnFamily<ByteBuffer, ByteBuffer> CF_COMPOSITE_CSV = ColumnFamily
            .newColumnFamily(
                    "CompositeCsv", 
                    ByteBufferSerializer.get(),
                    ByteBufferSerializer.get());

    public static ColumnFamily<MockCompositeType, String> CF_COMPOSITE_KEY = ColumnFamily
            .newColumnFamily(
                    "CompositeKey",
                    M_SERIALIZER, 
                    StringSerializer.get());

    public static ColumnFamily<String, UUID> CF_TIME_UUID = ColumnFamily
            .newColumnFamily(
                    "TimeUUID1", 
                    StringSerializer.get(),
                    TimeUUIDSerializer.get());

    public static AnnotatedCompositeSerializer<SessionEvent> SE_SERIALIZER = new AnnotatedCompositeSerializer<SessionEvent>(
            SessionEvent.class);

    public static ColumnFamily<String, SessionEvent> CF_CLICK_STREAM = ColumnFamily
            .newColumnFamily("ClickStream", StringSerializer.get(),
                    SE_SERIALIZER);

    private static final String SEEDS = "localhost:9160";
    private static final long   CASSANDRA_WAIT_TIME = 3000;
    private static String TEST_CLUSTER_NAME  = "cass_sandbox";
    private static String TEST_KEYSPACE_NAME = "AstyanaxUnitTests";

    
    @BeforeClass
    public static void setup() throws Exception {
        System.out.println("TESTING THRIFT KEYSPACE");

        SingletonEmbeddedCassandra.getInstance();
        
        Thread.sleep(CASSANDRA_WAIT_TIME);
        
        createKeyspace();
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (keyspaceContext != null)
            keyspaceContext.shutdown();
        
        Thread.sleep(CASSANDRA_WAIT_TIME);
    }

    public static void createKeyspace() throws Exception {
        keyspaceContext = new AstyanaxContext.Builder()
                .forCluster(TEST_CLUSTER_NAME)
                .forKeyspace(TEST_KEYSPACE_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                                .setDiscoveryDelayInSeconds(60000))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME
                                + "_" + TEST_KEYSPACE_NAME)
                                .setSocketTimeout(30000)
                                .setMaxTimeoutWhenExhausted(2000)
                                .setMaxConnsPerHost(20)
                                .setInitConnsPerHost(10)
                                .setSeeds(SEEDS)
                                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        keyspaceContext.start();
        
        keyspace = keyspaceContext.getClient();
        
        try {
            keyspace.dropKeyspace();
        }
        catch (Exception e) {
            LOG.info(e.getMessage());
        }
        
        ImmutableMap<String, Object> ksOptions = ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                        .put("replication_factor", "1")
                        .build())
                .put("strategy_class",     "SimpleStrategy")
                .build();
        
        ImmutableMap<String, Object> NO_OPTIONS = ImmutableMap.of();
        
        Map<ColumnFamily, Map<String, Object>> cfs = ImmutableMap.<ColumnFamily, Map<String, Object>>builder()
                .put(CF_STANDARD1, 
                     ImmutableMap.<String, Object>builder()
                         .put("column_metadata", ImmutableMap.<String, Object>builder()
                             .put("Index1", ImmutableMap.<String, Object>builder()
                                 .put("validation_class", "UTF8Type")
                                 .put("index_type",       "KEYS")
                                 .build())
                             .put("Index2", ImmutableMap.<String, Object>builder()
                                 .put("validation_class", "UTF8Type")
                                 .put("index_type",       "KEYS")
                                 .build())
                             .build())
                         .build())
                .put(CF_TTL,        NO_OPTIONS)
                .build();
        keyspace.createKeyspace(ksOptions, cfs);
        
        keyspace.createColumnFamily(CF_STANDARD2,  null);
        keyspace.createColumnFamily(CF_LONGCOLUMN, null);
        keyspace.createColumnFamily(CF_DELETE,     null);
        keyspace.createColumnFamily(ATOMIC_UPDATES,null);
        
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
        keyspace.createColumnFamily(CF_USER_INFO,         null);
        keyspace.createColumnFamily(CF_USERS, ImmutableMap.<String, Object>builder()
                .put("default_validation_class", "UTF8Type")
                .put("column_metadata", ImmutableMap.<String, Object>builder()
                        .put("firstname",  ImmutableMap.<String, Object>builder()
                                .put("validation_class", "UTF8Type")
                                .put("index_type",       "KEYS")
                                .build())
                        .put("lastname", ImmutableMap.<String, Object>builder()
                                .put("validation_class", "UTF8Type")
                                .put("index_type",       "KEYS")
                                .build())
                        .put("age", ImmutableMap.<String, Object>builder()
                                .put("validation_class", "LongType")
                                .put("index_type",       "KEYS")
                                .build())
                        .build())
                     .build());
        
        KeyspaceDefinition ki = keyspaceContext.getClient().describeKeyspace();
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
    public void testMultiColumnDelete() throws Exception {
        MutationBatch mb = keyspace.prepareMutationBatch();
        mb.withRow(CF_DELETE, 1L)
            .setTimestamp(1).putEmptyColumn(1L, null)
            .setTimestamp(10).putEmptyColumn(2L, null)
            ;
        mb.execute();
        
        ColumnList<Long> result1 = keyspace.prepareQuery(CF_DELETE).getRow(1L).execute().getResult();
        Assert.assertEquals(2, result1.size());
        Assert.assertNotNull(result1.getColumnByName(1L));
        Assert.assertNotNull(result1.getColumnByName(2L));
        
        logColumnList("Insert", result1);
        
        mb = keyspace.prepareMutationBatch();
        mb.withRow(CF_DELETE,  1L)
            .setTimestamp(result1.getColumnByName(1L).getTimestamp()-1)
            .deleteColumn(1L)
            .setTimestamp(result1.getColumnByName(2L).getTimestamp()-1)
            .deleteColumn(2L)
            .putEmptyColumn(3L, null);
        
        mb.execute();
        
        result1 = keyspace.prepareQuery(CF_DELETE).getRow(1L).execute().getResult();
        logColumnList("Delete with older timestamp", result1);
        Assert.assertEquals(3, result1.size());
        
        LOG.info("Delete L2 with TS: " + (result1.getColumnByName(2L).getTimestamp()+1));
        mb.withRow(CF_DELETE,  1L)
            .setTimestamp(result1.getColumnByName(1L).getTimestamp()+1)
            .deleteColumn(1L)
            .setTimestamp(result1.getColumnByName(2L).getTimestamp()+1)
            .deleteColumn(2L);
        mb.execute();
        
        result1 = keyspace.prepareQuery(CF_DELETE).getRow(1L).execute().getResult();
        logColumnList("Delete with newer timestamp", result1);
        Assert.assertEquals(1, result1.size());
    }
    
    <T> void logColumnList(String label, ColumnList<T> cl) {
        LOG.info(">>>>>> " + label);
        for (Column<T> c : cl) {
            LOG.info(c.getName() + " " + c.getTimestamp());
        }
        LOG.info("<<<<<<");
    }
    
    @Test
    public void testCqlComposite() throws Exception {
        CqlStatementResult result = keyspace.prepareCqlStatement()
            .withCql("SELECT * FROM " + CF_COMPOSITE_CSV.getName())
            .execute()
            .getResult();
        
        result.getSchema();
        result.getRows(CF_COMPOSITE_CSV);
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
        
        for (FieldMetadata field : def.getFieldsMetadata()) {
            LOG.info(field.getName() + " = " + def.getFieldValue(field.getName()) + " (" + field.getType() + ")");
        }
        
        for (ColumnFamilyDefinition cfDef : def.getColumnFamilyList()) {
            LOG.info("----------" );
            for (FieldMetadata field : cfDef.getFieldsMetadata()) {
                LOG.info(field.getName() + " = " + cfDef.getFieldValue(field.getName()) + " (" + field.getType() + ")");
            }
        }
    }
    
    @Test 
    public void testCopyKeyspace() throws Exception {
        KeyspaceDefinition def = keyspaceContext.getEntity().describeKeyspace();
        Properties props = def.getProperties();
        
        for (Entry<Object, Object> prop : props.entrySet()) {
            LOG.info(prop.getKey() + " : " + prop.getValue());
        }
        
        KsDef def2 = ThriftUtils.getThriftObjectFromProperties(KsDef.class, props);
        Properties props2 = ThriftUtils.getPropertiesFromThrift(def2);

        LOG.info("Props1:" + new TreeMap<Object, Object>(props));
        LOG.info("Props2:" + new TreeMap<Object, Object>(props2));
        MapDifference<Object, Object> diff = Maps.difference(props,  props2);
        LOG.info("Not copied : " + diff.entriesOnlyOnLeft());
        LOG.info("Added      : " + diff.entriesOnlyOnRight());
        LOG.info("Differing  : " + diff.entriesDiffering());
        
        
        Assert.assertTrue(diff.areEqual());
    }
    
    @Test
    public void testNonExistentKeyspace()  {
        AstyanaxContext<Keyspace> ctx = new AstyanaxContext.Builder()
            .forCluster(TEST_CLUSTER_NAME)
            .forKeyspace(TEST_KEYSPACE_NAME + "_NonExistent")
            .withAstyanaxConfiguration(
                    new AstyanaxConfigurationImpl()
                            .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                            .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                            .setDiscoveryDelayInSeconds(60000))
            .withConnectionPoolConfiguration(
                    new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME
                            + "_" + TEST_KEYSPACE_NAME)
                            .setSocketTimeout(30000)
                            .setMaxTimeoutWhenExhausted(2000)
                            .setMaxConnsPerHost(20)
                            .setInitConnsPerHost(10)
                            .setSeeds(SEEDS))
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());        
        
        ctx.start();
        
        try {
            KeyspaceDefinition keyspaceDef = ctx.getEntity().describeKeyspace();
            Assert.fail();
        } catch (ConnectionException e) {
            LOG.info(e.getMessage());
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
        Assert.assertEquals(1, hosts.size());
    }
    
    @Test
    public void paginateColumns() throws Exception {
        String column = "";
        ColumnList<String> columns;
        int pageize = 10;
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
        AtomicLong counter = new AtomicLong(0);
        try {
            OperationResult<Rows<String, String>> rows = keyspace
                    .prepareQuery(CF_STANDARD1).getAllRows().setConcurrencyLevel(2).setRowLimit(10)
                    .withColumnRange(new RangeBuilder().setLimit(0).build())
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
            Assert.assertEquals(27, counter.get());
        } catch (ConnectionException e) {
            Assert.fail();
        }
    }
    
    @Test
    public void getAllWithCallback() {
        try {
            final AtomicLong counter = new AtomicLong();

            keyspace.prepareQuery(CF_STANDARD1).getAllRows().setRowLimit(3)
                    .setRepeatLastToken(false)
                    .setConcurrencyLevel(2)
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
            Assert.assertEquals(27,  counter.get());
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
            ColumnQuery<String> query = keyspace.prepareQuery(CF_STANDARD1).getKey(key)
                    .getColumn(column);
            
            String v = query.execute().getResult().getStringValue();
            Assert.assertEquals(value, v);
            
            v = query.execute().getResult().getStringValue();
            Assert.assertEquals(value, v);

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
            } catch (NotFoundException e) {
            	// expected
            }
        }

        // Set a double value
        {
            String column = "IntColumn";
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
    public void testAtomicBatchMutation() throws Exception {
        MutationBatch mb = keyspace.prepareMutationBatch().withAtomicBatch(true);
        
        mb.withRow(ATOMIC_UPDATES, 1L)
            .putColumn(11L, 11L)
            .putColumn(12L, 12L);
        mb.withRow(ATOMIC_UPDATES, 2L)
            .putColumn(21L, 21L)
            .putColumn(22L, 22L);
        
        mb.execute();
        
        Rows<Long, Long> result = 
                keyspace.prepareQuery(ATOMIC_UPDATES).getAllRows().execute().getResult();
        
        int size = 0;

        for (Row<Long, Long> row : result) {
            LOG.info("ROW: " + row.getKey() + " " + row.getColumns().size());
            size++;
            Assert.assertEquals(2, row.getColumns().size());
        }
        Assert.assertEquals(2, size);
        
        size = 0;
        mb = keyspace.prepareMutationBatch().withAtomicBatch(true);
        
        mb.withRow(ATOMIC_UPDATES, 3L)
            .putColumn(11L, 11L)
            .putColumn(12L, 12L);
        mb.withRow(ATOMIC_UPDATES, 1L).delete();
        mb.withRow(ATOMIC_UPDATES, 2L).delete();
        
        mb.execute();

        result = keyspace.prepareQuery(ATOMIC_UPDATES).getAllRows().execute().getResult();

        for (Row<Long, Long> row : result) {
            LOG.info("ROW: " + row.getKey() + " " + row.getColumns().size());
            size++;
            Assert.assertEquals(2, row.getColumns().size());
        }
        Assert.assertEquals(1, size);
        
       mb = keyspace.prepareMutationBatch().withAtomicBatch(true);
       mb.withRow(ATOMIC_UPDATES, 3L).delete();
       mb.execute();
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
    public void testCql() {
        try {
            System.out.println("testCQL");
            LOG.info("CQL Test");
            OperationResult<CqlResult<String, String>> result = keyspace
                    .prepareQuery(CF_STANDARD1)
                    .withCql("SELECT * FROM Standard1;").execute();
            Assert.assertTrue(result.getResult().hasRows());
            Assert.assertEquals(29, result.getResult().getRows().size());
            Assert.assertFalse(result.getResult().hasNumber());
            
            Row<String, String> row;
            
            row = result.getResult().getRows().getRow("A");
            Assert.assertEquals("A", row.getKey());
            
            row = result.getResult().getRows().getRow("B");
            Assert.assertEquals("B", row.getKey());
            
            row = result.getResult().getRows().getRow("NonExistent");
            Assert.assertNull(row);
            
            row = result.getResult().getRows().getRowByIndex(9);
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
    public void testNullKeyInMutation() throws ConnectionException {
        try {
            keyspace.prepareMutationBatch()
                .withRow(CF_STANDARD1,  null)
                .putColumn("abc", "def");
            
            Assert.fail();
        }
        catch (NullPointerException e) {
            
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
    
    // This test confirms the fix for https://github.com/Netflix/astyanax/issues/170
    @Test
    public void columnAutoPaginateTest() throws Exception {
        final ColumnFamily<String, UUID> CF1 = ColumnFamily.newColumnFamily("CF1", StringSerializer.get(),
                TimeUUIDSerializer.get());
        final ColumnFamily<String, String> CF2 = ColumnFamily.newColumnFamily("CF2", StringSerializer.get(),
                StringSerializer.get());
        
        keyspace.createColumnFamily(CF1, null);
        Thread.sleep(3000);
        keyspace.createColumnFamily(CF2, null);
        Thread.sleep(3000);
    
        // query on another column family with different column key type
        // does not seem to work after the first query
        keyspace.prepareQuery(CF2).getKey("anything").execute();

        MutationBatch m = keyspace.prepareMutationBatch();
        m.withRow(CF1, "test").putColumn(TimeUUIDUtils.getUniqueTimeUUIDinMillis(), "value1", null);
        m.execute();
    
        RowQuery<String, UUID> query = keyspace.prepareQuery(CF1).getKey("test").autoPaginate(true);
    
        // Adding a column range removes the problem
        // query.withColumnRange(new RangeBuilder().build());
    
        ColumnList<UUID> columns = query.execute().getResult();
        
        keyspace.prepareQuery(CF2).getKey("anything").execute();
    }
    
    @Test
    public void testDDLWithProperties() throws Exception {
        String keyspaceName = "DDLPropertiesKeyspace";
        
        Properties props = new Properties();
        props.put("name", keyspaceName + "_wrong");
        props.put("strategy_class", "SimpleStrategy");
        props.put("strategy_options.replication_factor", "1");
        
        AstyanaxContext<Keyspace> kc = new AstyanaxContext.Builder()
            .forCluster(TEST_CLUSTER_NAME)
            .forKeyspace(keyspaceName)
            .withAstyanaxConfiguration(
                    new AstyanaxConfigurationImpl()
                            .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                            .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                            .setDiscoveryDelayInSeconds(60000))
            .withConnectionPoolConfiguration(
                    new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME + "_" + keyspaceName)
                            .setSocketTimeout(30000)
                            .setMaxTimeoutWhenExhausted(2000)
                            .setMaxConnsPerHost(20)
                            .setInitConnsPerHost(10)
                            .setSeeds(SEEDS)
                            )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());
        
        kc.start();

        Keyspace ks = kc.getClient();
        
        try {
            ks.createKeyspace(props);
            Assert.fail("Should have gotten name mismatch error");
        }
        catch (BadRequestException e) {
            LOG.info(e.getMessage());
        }
        
        props.put("name", keyspaceName);
        ks.createKeyspace(props);
        
        Properties props1 = ks.getKeyspaceProperties();
        
        LOG.info(props.toString());
        LOG.info(props1.toString());
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

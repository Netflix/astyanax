package com.netflix.astyanax.cql.test.todo;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.apache.cassandra.utils.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.RowCallback;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.SerializerPackageImpl;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UnknownComparatorException;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.ColumnarRecordWriter;
import com.netflix.astyanax.util.CsvRecordReader;
import com.netflix.astyanax.util.JsonRowsWriter;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.RecordReader;
import com.netflix.astyanax.util.RecordWriter;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;

public class OldTests {

    private static Logger LOG = LoggerFactory.getLogger(OldTests.class);

    private static Keyspace                  keyspace;
    private static AstyanaxContext<Keyspace> keyspaceContext;

    private static ColumnFamily<Long, String> CF_USERS = ColumnFamily
            .newColumnFamily(
                    "users", 
                    LongSerializer.get(),
                    StringSerializer.get());

    public static ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
            .newColumnFamily(
                    "Standard1", 
                    StringSerializer.get(),
                    StringSerializer.get());

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
                .build();
        keyspace.createKeyspace(ksOptions, cfs);
        
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

        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
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
}

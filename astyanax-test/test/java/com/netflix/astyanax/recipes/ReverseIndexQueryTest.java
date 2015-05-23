package com.netflix.astyanax.recipes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.recipes.ReverseIndexQuery.IndexEntryCallback;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class ReverseIndexQueryTest {

    private static Logger LOG = LoggerFactory.getLogger(ReverseIndexQueryTest.class);

    private static AstyanaxContext<Cluster> clusterContext;

    private static final String TEST_CLUSTER_NAME = "TestCluster";
    private static final String TEST_KEYSPACE_NAME = "ReverseIndexTest";
    private static final String TEST_DATA_CF = "Data";
    private static final String TEST_INDEX_CF = "Index";

    private static final boolean TEST_INIT_KEYSPACE = true;
    private static final long ROW_COUNT = 1000;
    private static final int SHARD_COUNT = 11;

    public static final String SEEDS = "localhost:7102";

    private static ColumnFamily<Long, String> CF_DATA = ColumnFamily
            .newColumnFamily(TEST_DATA_CF, LongSerializer.get(),
                    StringSerializer.get());

    private static class IndexEntry {
        @Component(ordinal = 0)
        Long value;
        @Component(ordinal = 1)
        Long key;

        public IndexEntry(Long value, Long key) {
            this.value = value;
            this.key = key;
        }
    }

    private static Serializer<IndexEntry> indexEntitySerializer = new AnnotatedCompositeSerializer<IndexEntry>(
            IndexEntry.class);

    private static ColumnFamily<String, IndexEntry> CF_INDEX = ColumnFamily
            .newColumnFamily(TEST_INDEX_CF, StringSerializer.get(),
                    indexEntitySerializer);

    @BeforeClass
    public static void setup() throws Exception {
        clusterContext = new AstyanaxContext.Builder()
                .forCluster(TEST_CLUSTER_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME)
                                .setMaxConnsPerHost(1).setSeeds(SEEDS))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildCluster(ThriftFamilyFactory.getInstance());

        clusterContext.start();

        if (TEST_INIT_KEYSPACE) {
            Cluster cluster = clusterContext.getEntity();
            try {
                LOG.info("Dropping keyspace: " + TEST_KEYSPACE_NAME);
                cluster.dropKeyspace(TEST_KEYSPACE_NAME);
                Thread.sleep(10000);
            } catch (ConnectionException e) {
                LOG.warn(e.getMessage());
            }

            Map<String, String> stratOptions = new HashMap<String, String>();
            stratOptions.put("replication_factor", "3");

            try {
                LOG.info("Creating keyspace: " + TEST_KEYSPACE_NAME);

                KeyspaceDefinition ksDef = cluster.makeKeyspaceDefinition();

                ksDef.setName(TEST_KEYSPACE_NAME)
                        .setStrategyOptions(stratOptions)
                        .setStrategyClass("SimpleStrategy")
                        .addColumnFamily(
                                cluster.makeColumnFamilyDefinition()
                                        .setName(CF_DATA.getName())
                                        .setComparatorType("UTF8Type")
                        // .setKeyValidationClass("LongType")
                        // .setDefaultValidationClass("BytesType")
                        )
                        .addColumnFamily(
                                cluster.makeColumnFamilyDefinition()
                                        .setName(CF_INDEX.getName())
                                        .setComparatorType(
                                                "CompositeType(LongType, LongType)")
                                        .setDefaultValidationClass("BytesType"));
                cluster.addKeyspace(ksDef);
                Thread.sleep(2000);
                populateKeyspace();
            } catch (ConnectionException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    @AfterClass
    public static void teardown() {
        if (clusterContext != null)
            clusterContext.shutdown();
    }

    public static void populateKeyspace() throws Exception {
        LOG.info("Ppoulating keyspace: " + TEST_KEYSPACE_NAME);

        Keyspace keyspace = clusterContext.getEntity().getKeyspace(
                TEST_KEYSPACE_NAME);

        try {
            // CF_Users :
            // 1 :
            // 'A' : 1,
            // 'B' : 2,
            //
            // CF_Index :
            // 'B_Shard1':
            // 2:1 : null
            // 3:2 : null
            //

            MutationBatch m = keyspace.prepareMutationBatch();

            for (long row = 0; row < ROW_COUNT; row++) {
                long value = row * 100;
                m.withRow(CF_DATA, row).putColumn("A", "ABC", null)
                        .putColumn("B", "DEF", null);
                m.withRow(CF_INDEX, "B_" + (row % SHARD_COUNT)).putColumn(
                        new IndexEntry(value, row), row, null);
            }

            // System.out.println(m);
            m.execute();
        } catch (Exception e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testReverseIndex() throws Exception{
        LOG.info("Starting");
        final AtomicLong counter = new AtomicLong();

        Keyspace keyspace = clusterContext.getEntity().getKeyspace(TEST_KEYSPACE_NAME);
        ReverseIndexQuery
                .newQuery(keyspace, CF_DATA, CF_INDEX.getName(),
                        LongSerializer.get())
                .fromIndexValue(100L)
                .toIndexValue(10000L)
                .withIndexShards(
                        new Shards.StringShardBuilder().setPrefix("B_")
                                .setShardCount(SHARD_COUNT).build())
                .withColumnSlice(Arrays.asList("A"))
                .forEach(new Function<Row<Long, String>, Void>() {
                    @Override
                    public Void apply(Row<Long, String> row) {
                        StringBuilder sb = new StringBuilder();
                        for (Column<String> column : row.getColumns()) {
                            sb.append(column.getName()).append(", ");
                        }
                        counter.incrementAndGet();
                        LOG.info("Row: " + row.getKey() + " Columns: "
                                + sb.toString());
                        return null;
                    }
                }).forEachIndexEntry(new IndexEntryCallback<Long, Long>() {
                    @Override
                    public boolean handleEntry(Long key, Long value,
                            ByteBuffer meta) {
                        LOG.info("Row : " + key + " IndexValue: " + value
                                + " Meta: "
                                + LongSerializer.get().fromByteBuffer(meta));
                        if (key % 2 == 1)
                            return false;
                        return true;
                    }
                }).execute();

        LOG.info("Read " + counter.get() + " rows");
    }

}

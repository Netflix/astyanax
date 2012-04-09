package com.netflix.astyanax.recipes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
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
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.BagConnectionPoolImplTest;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.recipes.ReverseIndexQuery.IndexEntryCallback;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

public class ReversedIterationTest {

    private static Logger LOG = LoggerFactory.getLogger(ReversedIterationTest.class);

    private static AstyanaxContext<Cluster> clusterContext;

    private static final String TEST_CLUSTER_NAME = "TestCluster";
    private static final String TEST_KEYSPACE_NAME = "ReversedIterationTest";
    private static final String TEST_DATA_CF = "Data";
    private static final String TEST_INDEX_CF = "Index";

    private static final boolean TEST_INIT_KEYSPACE = true;
    private static final long ROW_COUNT = 1000;
    private static final int SHARD_COUNT = 11;

    public static final String SEEDS = "localhost:7102";

    public static final List<String> LETTERS = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");

    private static ColumnFamily<String, String> CF_DATA = ColumnFamily.newColumnFamily(
        TEST_DATA_CF, StringSerializer.get(), StringSerializer.get());

    @BeforeClass
    public static void setup() throws Exception {
        clusterContext = new AstyanaxContext.Builder()
            .forCluster(TEST_CLUSTER_NAME)
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
            .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME).setMaxConnsPerHost(1).setSeeds(SEEDS))
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

                ColumnFamilyDefinition cfDef = cluster.makeColumnFamilyDefinition()
                    .setName(CF_DATA.getName())
                    .setComparatorType("UTF8Type");

                KeyspaceDefinition ksDef = cluster.makeKeyspaceDefinition()
                    .setName(TEST_KEYSPACE_NAME)
                    .setStrategyOptions(stratOptions)
                    .setStrategyClass("SimpleStrategy")
                    .addColumnFamily(cfDef);

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
        if (clusterContext != null) {
            clusterContext.shutdown();
        }
    }

    public static void populateKeyspace() throws Exception {
        Keyspace keyspace = clusterContext.getEntity().getKeyspace(TEST_KEYSPACE_NAME);

        try {
            MutationBatch m = keyspace.prepareMutationBatch();
            ColumnListMutation<String> cm = m.withRow(CF_DATA, "letters");

            for (String letter : LETTERS) {
                cm.putColumn(letter, letter.toUpperCase(), null);
            }

            m.execute();
        } catch (Exception e) {
            LOG.error(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testReversedIteration() throws Exception {
        Keyspace keyspace = clusterContext.getEntity().getKeyspace(TEST_KEYSPACE_NAME);
        RowQuery<String, String> query = keyspace.prepareQuery(CF_DATA).getKey("letters")
            .autoPaginate(true)
            .withColumnRange(new RangeBuilder().setMaxSize(10).setReversed(true).build());
        List<String> columnOrder = new ArrayList<String>();
        ColumnList<String> columns;
        while (!(columns = query.execute().getResult()).isEmpty()) {
            for (Column<String> c : columns) {
                columnOrder.add(c.getName());
            }
        }
        List<String> reverseLetters = new ArrayList<String>(LETTERS);
        Collections.reverse(reverseLetters);
        Assert.assertEquals(reverseLetters, columnOrder);
    }

    @Test
    public void testIteration() throws Exception {
        Keyspace keyspace = clusterContext.getEntity().getKeyspace(TEST_KEYSPACE_NAME);
        RowQuery<String, String> query = keyspace.prepareQuery(CF_DATA).getKey("letters")
            .autoPaginate(true)
            .withColumnRange(new RangeBuilder().setMaxSize(10).build());
        List<String> columnOrder = new ArrayList<String>();
        ColumnList<String> columns;
        while (!(columns = query.execute().getResult()).isEmpty()) {
            for (Column<String> c : columns) {
                columnOrder.add(c.getName());
            }
        }
        Assert.assertEquals(LETTERS, columnOrder);
    }
}

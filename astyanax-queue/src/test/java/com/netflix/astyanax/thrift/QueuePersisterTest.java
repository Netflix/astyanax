package com.netflix.astyanax.thrift;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatchManager;
import com.netflix.astyanax.SingleMutationBatchManager;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.entitystore.CompositeEntityManager;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.queue.CountingQueueStats;
import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageProducer;
import com.netflix.astyanax.recipes.queue.MessageProducerImpl;
import com.netflix.astyanax.recipes.queue.MessageQueueException;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;
import com.netflix.astyanax.recipes.queue.MessageQueueManager;
import com.netflix.astyanax.recipes.queue.ShardedDistributedMessageQueue;
import com.netflix.astyanax.recipes.queue.SimpleMessageQueueManager;
import com.netflix.astyanax.recipes.queue.entity.MessageMetadataEntry;
import com.netflix.astyanax.recipes.queue.entity.MessageMetadataEntryType;
import com.netflix.astyanax.recipes.queue.persist.MessageMetadataPersister;
import com.netflix.astyanax.recipes.queue.persist.MessagePersister;
import com.netflix.astyanax.recipes.queue.persist.MessageQueuePersister;
import com.netflix.astyanax.recipes.queue.shard.QueueShardPolicy;
import com.netflix.astyanax.recipes.queue.shard.SingleQueueShardPolicy;
import com.netflix.astyanax.recipes.queue.shard.TimePartitionQueueShardPolicy;
import com.netflix.astyanax.recipes.queue.triggers.RunOnceTrigger;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class QueuePersisterTest {

    private static Logger LOG = LoggerFactory.getLogger(QueueTest.class);
    private static Keyspace keyspace;
    private static AstyanaxContext<Keyspace> keyspaceContext;
    private static String TEST_CLUSTER_NAME = "Cluster1";
    private static String TEST_KEYSPACE_NAME = "AstyanaxUnitTests";
    private static final String SEEDS = "localhost:9160";
    private static final long CASSANDRA_WAIT_TIME = 3000;
    private static final int TTL = 20;
    private static final int TIMEOUT = 10;
    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.CL_ONE;
    private ReentrantLockManager slm = null;
    private String qNameSfx = null;

    private static MessageQueueManager manager;
    
    @BeforeClass
    public static void setup() throws Exception {
        LOG.info("TESTING THRIFT KEYSPACE");

        SingletonEmbeddedCassandra.getInstance();

        Thread.sleep(CASSANDRA_WAIT_TIME);

        createKeyspace();
    }

    public static void createKeyspace() throws Exception {
        keyspaceContext = new AstyanaxContext.Builder()
                .forCluster(TEST_CLUSTER_NAME)
                .forKeyspace(TEST_KEYSPACE_NAME)
                .withAstyanaxConfiguration(
            new AstyanaxConfigurationImpl()
                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
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

        keyspaceContext.start();

        keyspace = keyspaceContext.getClient();

        try {
            keyspace.dropKeyspace();
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }

        keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                .put("replication_factor", "1")
                .build())
                .put("strategy_class", "SimpleStrategy")
                .build());

        final CountingQueueStats stats = new CountingQueueStats();

//        manager = SimpleMessageQueueManager.builder()
//                    .withKeyspace(keyspace)
//                    .withConsistencyLevel(CONSISTENCY_LEVEL)
//                    .build();
//        manager.createStorage();
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (keyspaceContext != null) {
            keyspaceContext.shutdown();
        }

        Thread.sleep(CASSANDRA_WAIT_TIME);
    }
    
    @Test
    public void testQueuePersister() throws Exception {
//        MessageQueueInfo queueInfo = MessageQueueInfo.builder()
//                .withQueueName("testQueuePersister")
//                .build();
//        
//        SingleQueueShardPolicy shardPolicy  = new SingleQueueShardPolicy(queueInfo, "fixed");
//        MutationBatchManager   batchManager = new SingleMutationBatchManager(keyspace, ConsistencyLevel.CL_ONE);
//        MessageQueuePersister  persister    = new MessageQueuePersister(keyspace, batchManager, shardPolicy, queueInfo);
//        
//        persister.createStorage();
//        
//        Collection<Message> messages = ImmutableList.of(
//                new Message()
//                    .setKey("TestKey0")
//                    .setTrigger(RunOnceTrigger.builder().build()),
//                new Message()
//                    .setKey("TestKey1")
//                    .setTrigger(RunOnceTrigger.builder().build())
//            );
//        
//        persister.writeMessages(messages);
//        
//        batchManager.commitSharedMutationBatch();
//        
//        Collection<Message> m;
//        
//        m = persister.readMessages(shardPolicy.getShardKey(null), 1);
//        LOG.info(m.toString());
//        Assert.assertEquals(1, m.size());
//        
//        m = persister.readMessages(shardPolicy.getShardKey(null), 100);
//        LOG.info(m.toString());
//        Assert.assertEquals(2, m.size());
    }
    
    @Test
    public void testMetadataPersister() throws Exception {
        CompositeEntityManager<MessageMetadataEntry, String> entityManager = CompositeEntityManager.<MessageMetadataEntry, String>builder()
                .withKeyspace(keyspace)
                .withColumnFamily("testMetadataPersister_metadata")
                .withConsistency(CONSISTENCY_LEVEL)
                .withEntityType(MessageMetadataEntry.class)
                .build();
        
        entityManager.createStorage(null);
        String rowKey = "Test1";
        
        entityManager.put(MessageMetadataEntry.newUnique(rowKey, 120));
        entityManager.put(MessageMetadataEntry.newUnique(rowKey, 120));

        // Phase 2: Read back ALL the lock columms
        Map<String, Collection<MessageMetadataEntry>> result;
        result = entityManager.createNativeQuery()
                .whereId().equal(rowKey)
                .whereColumn("type").equal((byte)MessageMetadataEntryType.Unique.ordinal())
                .getResultSetById();
        
        LOG.info(result.toString());
    }
    
    @Test
    public void testHistoryPersister() {
        
    }
    
    
}

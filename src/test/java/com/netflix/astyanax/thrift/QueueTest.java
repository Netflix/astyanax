package com.netflix.astyanax.thrift;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.queue.CountingQueueStats;
import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageContext;
import com.netflix.astyanax.recipes.queue.MessageProducer;
import com.netflix.astyanax.recipes.queue.MessageQueue;
import com.netflix.astyanax.recipes.queue.MessageQueueDispatcher;
import com.netflix.astyanax.recipes.queue.MessageQueueException;
import com.netflix.astyanax.recipes.queue.ShardedDistributedMessageQueue;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;

public class QueueTest {
    private static Logger LOG = LoggerFactory.getLogger(QueueTest.class);
    
    private static Keyspace                  keyspace;
    private static AstyanaxContext<Keyspace> keyspaceContext;

    private static String TEST_CLUSTER_NAME  = "cass_sandbox";
    private static String TEST_KEYSPACE_NAME = "AstyanaxUnitTests";
    private static String SCHEDULER_NAME_CF_NAME = "SchedulerQueue";
    private static final String SEEDS = "localhost:9160";
    private static final long   CASSANDRA_WAIT_TIME = 3000;
    private static final int    TTL                 = 20;
    private static final int    TIMEOUT             = 10;

    @BeforeClass
    public static void setup() throws Exception {
        System.out.println("TESTING THRIFT KEYSPACE");

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
        
        keyspace = keyspaceContext.getEntity();
        
        try {
            keyspace.dropKeyspace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        
        keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                        .put("replication_factor", "1")
                        .build())
                .put("strategy_class",     "SimpleStrategy")
                .build()
                );
        
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (keyspaceContext != null)
            keyspaceContext.shutdown();
        
        Thread.sleep(CASSANDRA_WAIT_TIME);
    }
    
    @Test
    @Ignore
    public void testStressQueue() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(100);
        
        final AtomicLong counter = new AtomicLong(0);
        final AtomicLong insertCount = new AtomicLong(0);
        final int max_count = 100000;

        final CountingQueueStats stats = new CountingQueueStats();
        
        final ConsistencyLevel cl = ConsistencyLevel.CL_ONE;
        final MessageQueue scheduler = new ShardedDistributedMessageQueue.Builder()
            .withColumnFamily(SCHEDULER_NAME_CF_NAME)
            .withKeyspace(keyspace)
            .withConsistencyLevel(cl)
            .withStats(stats)
//            .withTimeBuckets(50,  30,  TimeUnit.SECONDS)
            .withShardCount(20)
            .withPollInterval(100L,  TimeUnit.MILLISECONDS)
            .build();
        
        scheduler.createStorage();
        Thread.sleep(1000);
        scheduler.createQueue();
        
        final ConcurrentMap<String, Boolean> lookup = Maps.newConcurrentMap();
        
        final int batchSize = 50;
        // Producer
        final AtomicLong iCounter = new AtomicLong(0);
        for (int j = 0; j < 1; j++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    MessageProducer producer = scheduler.createProducer();
                    
                    List<Message> tasks = Lists.newArrayList();
                    while (true) {
                        long count = insertCount.incrementAndGet();
                        if (count > max_count) {
                            insertCount.decrementAndGet();
                            break;
                        }
                        try {
                            tasks.add(new Message()
                                .setKey("" + count)
                                .addParameter("data", "The quick brown fox jumped over the lazy cow " + count)
    //                            .setNextTriggerTime(TimeUnit.SECONDS.convert(tm, TimeUnit.MILLISECONDS))
//                                .setTimeout(1L, TimeUnit.MINUTES)
                                );
                            
                            if (tasks.size() == batchSize) {
                                producer.sendMessages(tasks);
                                tasks.clear();
                            }
                        } catch (Exception e) {
                            LOG.error(e.getMessage());
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                        }
                    }
                    
                    if (tasks.size() == batchSize) {
                        try {
                            producer.sendMessages(tasks);
                        } catch (MessageQueueException e) {
                            e.printStackTrace();
                        }
                        tasks.clear();
                    }
                }
            });
        }
        
        // Status
        final AtomicLong prevCount = new AtomicLong(0);
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    long newCount = counter.get();
                    System.out.println("#### Processed : " + (newCount - prevCount.get()) + " of " + newCount + " (" + (insertCount.get() - newCount) + ")");
//                        System.out.println("#### Pending   : " + scheduler.getTaskCount());
//                        for (Entry<String, Integer> shard : producer.getShardCounts().entrySet()) {
//                            LOG.info("  " + shard.getKey() + " : " + shard.getValue());
//                        }
                    System.out.println(stats.toString());
                    prevCount.set(newCount);
                } 
                catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } 
            }
        }, 1, 1, TimeUnit.SECONDS);
        
        // Consumer
        MessageQueueDispatcher dispatcher = new MessageQueueDispatcher.Builder()
            .withBatchSize(50)
            .withCallback(new Function<MessageContext, Boolean>() {
                @Override
                public Boolean apply(MessageContext message) {
                    String data = (String)message.getMessage().getParameters().get("data");
                    counter.incrementAndGet();
                    // Return true to 'ack' the message
                    // Return false to not 'ack' which will result in the message timing out 
                    // Throw any exception to put the message into a poison queue
                    return true;
                }
             })
             .withMessageQueue(scheduler)
             .withConsumerCount(5)
             .withThreadCount(1 + 10)
             .build();

        dispatcher.start();
        
        executor.awaitTermination(1000,  TimeUnit.SECONDS);
    }
}

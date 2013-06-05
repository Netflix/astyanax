package com.netflix.astyanax.thrift;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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
import com.netflix.astyanax.recipes.queue.KeyExistsException;
import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageContext;
import com.netflix.astyanax.recipes.queue.MessageQueueDispatcher;
import com.netflix.astyanax.recipes.queue.MessageQueueManager;
import com.netflix.astyanax.recipes.queue.MessageQueueInfo;
import com.netflix.astyanax.recipes.queue.ShardLock;
import com.netflix.astyanax.recipes.queue.ShardedDistributedMessageQueue;
import com.netflix.astyanax.recipes.queue.SimpleMessageQueueManager;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;
import com.netflix.astyanax.recipes.queue.triggers.RepeatingTrigger;
import com.netflix.astyanax.recipes.queue.triggers.RunOnceTrigger;
import com.netflix.astyanax.util.SingletonEmbeddedCassandra;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import static org.junit.Assert.*;

@RunWith(value = Parameterized.class)
public class QueueTest {

    private static Logger LOG = LoggerFactory.getLogger(QueueTest.class);
    private static Keyspace keyspace;
    private static AstyanaxContext<Keyspace> keyspaceContext;
    private static String TEST_CLUSTER_NAME = "cass_sandbox";
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

    public QueueTest(ReentrantLockManager s, String sfx) {
        slm = s;
        qNameSfx = sfx;
        System.out.println((s == null? "Running without SLM":"Running WITH SLM") + " and suffix " + qNameSfx);
    }

    @Parameters
    public static Collection<Object[]> parameters() {
        Object[][] data = new Object[][]{{new ReentrantLockManager(), "WITHSLM"},{null, "NOSLM"}};
//        Object[][] data = new Object[][]{{new ReentrantLockManager(), "WITHSLM"}};
        return Arrays.asList(data);
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

        manager = SimpleMessageQueueManager.builder()
                    .withKeyspace(keyspace)
                    .withConsistencyLevel(CONSISTENCY_LEVEL)
                    .build();
        manager.createStorage();
        
        Properties props = keyspace.getKeyspaceProperties();
        LOG.info(props.toString());
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (keyspaceContext != null) {
            keyspaceContext.shutdown();
        }

        Thread.sleep(CASSANDRA_WAIT_TIME);
    }

    @Test
    public void testAdmin() throws MessageQueueException {
        manager.createMessageQueue(MessageQueueInfo.builder().withQueueName("TEST_ADMIN").build());
        
        List<MessageQueueInfo> queueList = manager.listMessageQueues();
        LOG.info(queueList.toString());
    }

    @Test
    public void testRepeatingMessage() throws Exception {
        final CountingQueueStats stats = new CountingQueueStats();

        final String queueName = "RepeatingMessageQueue" + qNameSfx;
        
        manager.createMessageQueue(MessageQueueInfo.builder().withQueueName(queueName).build());
        
        // Create a simple queue
        final ShardedDistributedMessageQueue queue = new ShardedDistributedMessageQueue.Builder()
                .withQueue(manager.readQueueInfo(queueName))
                .withKeyspace(keyspace)
                .withConsistencyLevel(CONSISTENCY_LEVEL)
                .withStats(stats)
                .withShardLockManager(slm)
                .build();

        // Enqueue a recurring message
        final String key = "RepeatingMessageWithTimeout";
        final Message message = new Message()
                .setUniqueKey(key)
                .setTimeout(1, TimeUnit.SECONDS)
                .setTrigger(new RepeatingTrigger.Builder().withInterval(1, TimeUnit.SECONDS).build());

        queue.sendMessage(message);

        // Make sure it's unique by trying to submit again
        try {
            queue.sendMessage(message);
            Assert.fail();
        } catch (KeyExistsException e) {
            LOG.info("Key already exists");
        }

        // Confirm that the message is there
        Assert.assertEquals(1, queue.getMessageCount());
        printMessages("Pending messages after insert ORIG message", queue.peekMessagesByKey(key));

        // Consume the message
        LOG.info("*** Reading first message ***");
        final Collection<MessageContext> m1 = queue.readMessages(10);
        printMessages("Consuming the ORIG message", m1);
        Assert.assertEquals(1, m1.size());

        printMessages("Pending messages after consume ORIG " + key, queue.peekMessagesByKey(key));

        // Exceed the timeout
        Thread.sleep(2000);

        // Consume the timeout event
        LOG.info("*** Reading timeout message ***");
        final Collection<MessageContext> m2 = queue.readMessages(10);
        printMessages("Consuming the TIMEOUT message", m2);
        Assert.assertEquals(1, m2.size());

        printMessages("Pending messages after consume TIMEOUT " + key, queue.peekMessagesByKey(key));
//        Assert.assertEquals(2, m2a.size());

        LOG.info("*** Acking both messages ***");
        queue.ackMessages(m1);
        queue.ackMessages(m2);

        printMessages("Pending messages after both acks " + key, queue.peekMessagesByKey(key));
//        Assert.assertEquals(2, m2a.size());

        // Consume anything that is in the queue
        final Collection<MessageContext> m3 = queue.readMessages(10);
        printMessages("Consuming messages", m3);
        Assert.assertEquals(1, m3.size());

        printMessages("Pending messages after 2nd consume " + key, queue.peekMessagesByKey(key));

        queue.ackMessages(m3);

        Thread.sleep(2000);

        final Collection<MessageContext> m4 = queue.readMessages(10);
        printMessages("Consuming messages", m4);
        Assert.assertEquals(1, m4.size());

        // There should be only one message
//        Assert.assertEquals(1, queue.getMessageCount());

        for (int i = 0; i < 10; i++) {
            final Collection<MessageContext> m5 = queue.readMessages(10);
            Assert.assertEquals(1, m5.size());

            long systemtime = System.currentTimeMillis();
            MessageContext m = Iterables.getFirst(m5, null);
            LOG.info("MessageTime: " + (systemtime - m.getMessage().getTrigger().getTriggerTime()));
            queue.ackMessages(m5);
        }
    }

    private <T> void printMessages(String caption, Collection<T> messages) {
        LOG.info(caption + "(" + messages.size() + ")");
        for (T message : messages) {
            LOG.info("   " + message);
        }
    }

    @Test
    public void testNoKeyQueue() throws Exception {
        final CountingQueueStats stats = new CountingQueueStats();

        final String queueName = "TestNoKeyQueue" + qNameSfx;
        
        manager.createMessageQueue(MessageQueueInfo.builder().withQueueName(queueName).build());

        final ShardedDistributedMessageQueue queue = new ShardedDistributedMessageQueue.Builder()
                .withQueue(manager.readQueueInfo(queueName))
                .withKeyspace(keyspace)
                .withConsistencyLevel(CONSISTENCY_LEVEL)
                .withStats(stats)
                .withShardLockManager(slm)
                .build();

        String key = "MyEvent";
        String key2 = "MyEvent2";

        {
            final Message m = new Message();

            // Add a message
            LOG.info(m.toString());
            MessageContext context = queue.sendMessage(m);
            LOG.info("MessageId: " + context);
        }
        
        Collection<MessageContext> messages = queue.readMessages(10);
        LOG.info(messages.toString());
        Assert.assertEquals(1, messages.size());
    }
//
//    @Test
//    public void testQueue() throws Exception {
//        final CountingQueueStats stats = new CountingQueueStats();
//
//        String queueName = "TestQueue" + qNameSfx;
//        
//        manager.createMessageQueue(MessageQueueInfo.builder().withQueueName(queueName).build());
//        final ShardedDistributedMessageQueue queue = new ShardedDistributedMessageQueue.Builder()
//                .withQueue(manager.readQueueInfo(queueName))
//                .withKeyspace(keyspace)
//                .withConsistencyLevel(CONSISTENCY_LEVEL)
//                .withStats(stats)
//                .withShardLockManager(slm)
//                .build();
//
//        String key = "MyEvent";
//        String key2 = "MyEvent2";
//
//        {
//            final Message m = new Message().setKey(key);
//
//            // Add a message
//            LOG.info(m.toString());
//            String messageId = producer.sendMessage(m);
//            LOG.info("MessageId: " + messageId);
//
//            Assert.assertEquals(1, queue.getMessageCount());
//
//            // Read it by the messageId
//            final Message m1rm = queue.peekMessage(messageId);
//            LOG.info("m1rm: " + m1rm);
//            Assert.assertNotNull(m1rm);
//
//            // Read it by the key
//            final Message m1rk = queue.peekMessageByKey(key);
//            LOG.info("m1rk:" + m1rk);
//            Assert.assertNotNull(m1rk);
//
//            // Delete the message
//            queue.deleteMessageByKey(key);
//
//            // Read and verify that it is gone
//            final Message m1rkd = queue.peekMessageByKey(key);
//            Assert.assertNull(m1rkd);
//
//            // Read and verify that it is gone
//            final Message m1rmd = queue.peekMessage(messageId);
//            Assert.assertNull(m1rmd);
//        }
//
//        {
//            // Send another message
//            final Message m = new Message().setUniqueKey(key);
//            LOG.info("m2: " + m);
//            final String messageId2 = producer.sendMessage(m);
//            LOG.info("MessageId2: " + messageId2);
//
//            try {
//                final Message m2 = new Message().setUniqueKey(key);
//                producer.sendMessage(m2);
//                Assert.fail("Message should already exists");
//            } catch (MessageQueueException e) {
//                LOG.info("Failed to insert duplicate key", e);
//            }
//
//            try {
//                List<Message> messages = Lists.newArrayList(
//                        new Message().setUniqueKey(key),
//                        new Message().setUniqueKey(key2));
//
//                PersistMessageResponse result = producer.sendMessages(messages);
//                Assert.assertEquals(1, result.getMessages().size());
//                Assert.assertEquals(1, result.getNotUnique().size());
//            } catch (MessageQueueException e) {
//                Assert.fail(e.getMessage());
//            }
//
//            Map<String, Integer> counts = queue.getShardCounts();
//            LOG.info(counts.toString());
//            Assert.assertEquals(2, queue.getMessageCount());
//
//            // Delete the message
//            queue.deleteMessageByKey(key2);
//
//            // Read the message
//            final Collection<MessageContext> lm2 = queue.readMessages(10, 10, TimeUnit.SECONDS);
//            LOG.info("Read message: " + lm2);
//            Assert.assertEquals(1, lm2.size());
//            LOG.info(lm2.toString());
//            Assert.assertEquals(1, queue.getMessageCount());
//
//            queue.ackMessages(lm2);
//            Assert.assertEquals(0, queue.getMessageCount());
//        }
//
//        {
//            final Message m = new Message()
//                    .setKey("Key12345")
//                    .setTrigger(new RepeatingTrigger.Builder()
//                    .withInterval(3, TimeUnit.SECONDS)
//                    .withRepeatCount(10)
//                    .build());
//            final String messageId3 = producer.sendMessage(m);
//            Assert.assertNotNull(messageId3);
//            final Message m3rm = queue.peekMessage(messageId3);
//            Assert.assertNotNull(m3rm);
//            LOG.info(m3rm.toString());
//            Assert.assertEquals(1, queue.getMessageCount());
//
//            queue.deleteMessage(messageId3);
//
//            Assert.assertEquals(0, queue.getMessageCount());
//        }
//
////        {
////            final String repeatingKey = "RepeatingMessage";
////            final Message m = new Message()
////                .setKey(repeatingKey)
////                .setKeepHistory(true)
////                .setTaskClass(HelloWorldFunction.class.getCanonicalName())
////                .setTrigger(new RepeatingTrigger.Builder()
////                    .withInterval(3,  TimeUnit.SECONDS)
////                    .withRepeatCount(5)
////                    .build());
////            final String messageId = producer.sendMessage(m);
////
////            final AtomicLong counter = new AtomicLong(0);
////
////            MessageQueueDispatcher dispatcher = new MessageQueueDispatcher.Builder()
////                .withBatchSize(5)
////                .withCallback(new Function<MessageContext, Boolean>() {
////                    long startTime = 0;
////
////                    @Override
////                    public synchronized Boolean apply(MessageContext message) {
////                        if (startTime == 0)
////                            startTime = System.currentTimeMillis();
////
////                        LOG.info("Callback : " + (System.currentTimeMillis() - startTime) + " " + message);
////                        counter.incrementAndGet();
////                        return true;
////                    }
////                })
////                .withMessageQueue(scheduler)
////                .withThreadCount(2)
////                .build();
////
////            dispatcher.start();
////
////            Thread.sleep(TimeUnit.MILLISECONDS.convert(20,  TimeUnit.SECONDS));
////
////            Collection<MessageHistory> history = scheduler.getKeyHistory(repeatingKey, null, null, 10);
////            LOG.info(history);
////
////            dispatcher.stop();
////
////            Assert.assertEquals(5,  counter.get());
////        }
//
//        // Add a batch of messages and peek
//        {
//            List<Message> messages = Lists.newArrayList();
//
//            for (int i = 0; i < 10; i++) {
//                messages.add(new Message().addParameter("body", "" + i));
//            }
//
//            producer.sendMessages(messages);
//
//            Collection<Message> all = queue.peekMessages(Integer.MAX_VALUE);
//            Assert.assertEquals(10, all.size());
//
//            for (Message msg : all) {
//                LOG.info(msg.getParameters().toString());
//            }
//        }
//
//    }

    @Test
    @Ignore
    public void testStressQueue() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(100);

        final AtomicLong counter = new AtomicLong(0);
        final AtomicLong insertCount = new AtomicLong(0);
        final long max_count = 1000000;

        final CountingQueueStats stats = new CountingQueueStats();

        String queueName = "StressQueue"+qNameSfx;
        
        manager.createMessageQueue(MessageQueueInfo.builder()
                .withQueueName(queueName)
                .withTimeBuckets(10, 30, TimeUnit.SECONDS)
                .withShardCount(100)
                .build());
        
        final ShardedDistributedMessageQueue queue = new ShardedDistributedMessageQueue.Builder()
                .withQueue(manager.readQueueInfo(queueName))
                .withKeyspace(keyspace)
                .withConsistencyLevel(CONSISTENCY_LEVEL)
                .withStats(stats)
                .withShardLockManager(slm)
                .build();

        Thread.sleep(1000);

        final ConcurrentMap<String, Boolean> lookup = Maps.newConcurrentMap();

        final int batchSize = 50;

        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < max_count / batchSize; i++) {
                    long tm = System.currentTimeMillis();
                    List<Message> messages = Lists.newArrayList();
                    for (int j = 0; j < batchSize; j++) {
                        long id = insertCount.incrementAndGet();
                        messages.add(new Message()
                                .setKey("" + id)
                                .addParameter("data", "The quick brown fox jumped over the lazy cow " + id)
                                .setTimeout(0)
                                .setTrigger(new RunOnceTrigger.Builder()
                                .withDelay(j, TimeUnit.SECONDS)
                                .build()));
                    }
                    try {
                        queue.sendMessages(messages);
                    } catch (MessageQueueException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                    long sleep = 1000 - System.currentTimeMillis() - tm;
                    if (sleep > 0) {
                        try {
                            Thread.sleep(sleep);
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }
            }
        });

//        // Producer
//        final AtomicLong iCounter = new AtomicLong(0);
//        for (int j = 0; j < 1; j++) {
//            executor.submit(new Runnable() {
//                @Override
//                public void run() {
//
//                    List<Message> tasks = Lists.newArrayList();
//                    while (true) {
//                        long count = insertCount.incrementAndGet();
//                        if (count > max_count) {
//                            insertCount.decrementAndGet();
//                            break;
//                        }
//                        try {
//                            tasks.add(new Message()
//                                .setKey("" + count)
//                                .addParameter("data", "The quick brown fox jumped over the lazy cow " + count)
//    //                            .setNextTriggerTime(TimeUnit.SECONDS.convert(tm, TimeUnit.MILLISECONDS))
////                                .setTimeout(1L, TimeUnit.MINUTES)
//                                );
//
//                            if (tasks.size() == batchSize) {
//                                queue.sendMessages(tasks);
//                                tasks.clear();
//                            }
//                        } catch (Exception e) {
//                            LOG.error(e.getMessage());
//                            try {
//                                Thread.sleep(1000);
//                            } catch (InterruptedException e1) {
//                                e1.printStackTrace();
//                            }
//                        }
//                    }
//
//                    if (tasks.size() == batchSize) {
//                        try {
//                            queue.sendMessages(tasks);
//                        } catch (MessageQueueException e) {
//                            e.printStackTrace();
//                        }
//                        tasks.clear();
//                    }
//                }
//            });
//        }

        // Status
        final AtomicLong prevCount = new AtomicLong(0);
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    long newCount = insertCount.get();
//                    long newCount = counter.get();
//                    LOG.info("#### Processed : " + (newCount - prevCount.get()) + " of " + newCount + " (" + (insertCount.get() - newCount) + ")");
//                        LOG.info("#### Pending   : " + scheduler.getTaskCount());
//                        for (Entry<String, Integer> shard : queue.getShardCounts().entrySet()) {
//                            LOG.info("  " + shard.getKey() + " : " + shard.getValue());
//                        }
                    LOG.info(stats.toString());
                    LOG.info("" + (newCount - prevCount.get()) + " /sec  (" + newCount + ")");
                    prevCount.set(newCount);

//                    if (insertCount.get() >= max_count) {
//                        Map<String, Integer> counts;
//                        try {
//                            counts = scheduler.getShardCounts();
//                            long total = 0;
//                            for (Entry<String, Integer> shard : counts.entrySet()) {
//                                total += shard.getValue();
//                            }
//
//                            LOG.info("Total: " + total + " " + counts.toString());
//                        } catch (MessageQueueException e) {
//                            // TODO Auto-generated catch block
//                            e.printStackTrace();
//                        }
//                    }

                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // Consumer
        MessageQueueDispatcher dispatcher = new MessageQueueDispatcher.Builder()
                .withBatchSize(500)
                .withCallback(new Function<MessageContext, Boolean>() {
                    @Override
                    public Boolean apply(MessageContext message) {
                        String data = (String) message.getMessage().getParameters().get("data");
                        counter.incrementAndGet();
                        // Return true to 'ack' the message
                        // Return false to not 'ack' which will result in the message timing out
                        // Throw any exception to put the message into a poison queue
                        return true;
                    }
                })
                .withMessageQueue(queue)
                .withConsumerCount(5)
                .withThreadCount(1 + 10)
                .build();

        dispatcher.start();

        executor.awaitTermination(1000, TimeUnit.SECONDS);
    }

    @Test
    public void testQueueBusyLock() throws Exception {

        final CountingQueueStats stats = new CountingQueueStats();

        final String queueName = "TestQueueBusyLock" + qNameSfx;
        
        manager.createMessageQueue(MessageQueueInfo.builder().withQueueName(queueName).build());
        
        final ShardedDistributedMessageQueue queue = new ShardedDistributedMessageQueue.Builder()
                .withQueue(manager.readQueueInfo(queueName))
                .withKeyspace(keyspace)
                .withConsistencyLevel(CONSISTENCY_LEVEL)
                .withStats(stats)
                .withShardLockManager(slm)
                .build();

        // Add a batch of messages and peek
        List<Message> messages = Lists.newArrayList();

        for (int i = 0; i < 5; i++) {
            messages.add(new Message().addParameter("body", "" + i));
        }

        queue.sendMessages(messages);
        long queuedCount = queue.getMessageCount();
        final AtomicInteger count = new AtomicInteger();

        // Lock the shard. This should throw a few BusyLockExceptions
        String shard = queue.getShardStats().keySet().iterator().next();
        ShardLock l = null;
        if(slm!=null) {
            l = slm.acquireLock(shard);
        }

        // Consumer
        MessageQueueDispatcher dispatcher = new MessageQueueDispatcher.Builder()
                .withBatchSize(25)
                .withCallback(new Function<MessageContext, Boolean>() {
                                    @Override
                                    public Boolean apply(MessageContext message) {
                                        count.incrementAndGet();
                                        return true;
                                    }
                                })
                .withMessageQueue(queue)
                .withConsumerCount(10)
                .withProcessorThreadCount(10)
                .withAckInterval(20, TimeUnit.MILLISECONDS)
                .withPollingInterval(15, TimeUnit.MILLISECONDS)
                .build();

        // Start the queue
        dispatcher.start();

        // Release the lock
        if(slm!=null) {
            // Wait
            Thread.sleep(1000);
            slm.releaseLock(l);
        }

        // Wait another 10 seconds and then stop the dispatcher
        Thread.sleep(1000);
        dispatcher.stop();

        assertEquals(queuedCount, count.intValue());
        // Check the busy lock count
        if(slm!=null) {
            System.out.println("Lock attempts " + slm.getLockAttempts());
            assertTrue(slm.getBusyLockCounts().get(shard).intValue() > 0);
        }
    }
    
}

package com.netflix.astyanax.thrift;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.netflix.astyanax.recipes.queue.KeyExistsException;
import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageConsumer;
import com.netflix.astyanax.recipes.queue.MessageContext;
import com.netflix.astyanax.recipes.queue.MessageProducer;
import com.netflix.astyanax.recipes.queue.MessageQueue;
import com.netflix.astyanax.recipes.queue.MessageQueueDispatcher;
import com.netflix.astyanax.recipes.queue.MessageQueueException;
import com.netflix.astyanax.recipes.queue.SendMessageResponse;
import com.netflix.astyanax.recipes.queue.ShardedDistributedMessageQueue;
import com.netflix.astyanax.recipes.queue.triggers.RepeatingTrigger;
import com.netflix.astyanax.recipes.queue.triggers.RunOnceTrigger;
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
    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.CL_ONE;

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
        
        final CountingQueueStats stats = new CountingQueueStats();
        
        final ShardedDistributedMessageQueue queue = new ShardedDistributedMessageQueue.Builder()
            .withColumnFamily(SCHEDULER_NAME_CF_NAME)
            .withQueueName("TestQueue")
            .withKeyspace(keyspace)
            .withConsistencyLevel(CONSISTENCY_LEVEL)
            .withStats(stats)
            .withTimeBuckets(2,  30,  TimeUnit.SECONDS)
            .withShardCount(2)
            .withPollInterval(100L,  TimeUnit.MILLISECONDS)
            .build();
        
        queue.createStorage();

    }

    @AfterClass
    public static void teardown() throws Exception {
        if (keyspaceContext != null)
            keyspaceContext.shutdown();
        
        Thread.sleep(CASSANDRA_WAIT_TIME);
    }
    
    @Test
    @Ignore
    // This tests for a known but that has yet to be fixed
    public void testRepeatingMessage() throws Exception {
        final CountingQueueStats stats = new CountingQueueStats();
        
        final ShardedDistributedMessageQueue queue = new ShardedDistributedMessageQueue.Builder()
            .withColumnFamily(SCHEDULER_NAME_CF_NAME)
            .withQueueName("RepeatingMessageQueue")
            .withKeyspace(keyspace)
            .withConsistencyLevel(CONSISTENCY_LEVEL)
            .withStats(stats)
            .withShardCount(1)
            .withPollInterval(100L,  TimeUnit.MILLISECONDS)
            .build();
    
        queue.createQueue();
        MessageProducer producer = queue.createProducer();
        MessageConsumer consumer = queue.createConsumer();

        // Enqueue a recurring messsage
        final String key = "MyMessage";
        final Message message = new Message()
            .setUniqueKey(key)
            .setTimeout(10, TimeUnit.SECONDS)
            .setTrigger(new RepeatingTrigger.Builder().withInterval(1, TimeUnit.SECONDS).build());
        
        producer.sendMessage(message);
        
        // Make sure it's unique by trying to submit again
        try {
            producer.sendMessage(message);
            Assert.fail();
        }
        catch (KeyExistsException e) {
        }
        
        // Confirm that the message is there
        Assert.assertEquals(1, queue.getMessageCount());
        List<Message> messages = queue.peekMessagesByKey(key);
        Assert.assertEquals(1, messages.size());
        
        // Consume the message
        List<MessageContext> m1 = consumer.readMessages(1);
        Assert.assertEquals(1, m1.size());
        
        // Exceed the timeout
        Thread.sleep(2000);
        
        // Consume the timeout event
        List<MessageContext> m2 = consumer.readMessages(1);
        Assert.assertEquals(1, m2.size());
        
        consumer.ackMessages(m1);
        consumer.ackMessages(m2);
        
        // There should be only one message
        Assert.assertEquals(1, queue.getMessageCount());
    }
    
    @Test
    public void testQueue() throws Exception {
        final CountingQueueStats stats = new CountingQueueStats();
        
        final ShardedDistributedMessageQueue scheduler = new ShardedDistributedMessageQueue.Builder()
            .withColumnFamily(SCHEDULER_NAME_CF_NAME)
            .withQueueName("TestQueue")
            .withKeyspace(keyspace)
            .withConsistencyLevel(CONSISTENCY_LEVEL)
            .withStats(stats)
            .withShardCount(1)
            .withPollInterval(100L,  TimeUnit.MILLISECONDS)
            .build();
        
        scheduler.createQueue();
        
        String key = "MyEvent";
        String key2 = "MyEvent2";
        
        MessageProducer producer = scheduler.createProducer();
        MessageConsumer consumer = scheduler.createConsumer();
        
        {
            final Message m = new Message().setKey(key);
            
            // Add a message
            System.out.println(m);
            String messageId = producer.sendMessage(m);
            System.out.println("MessageId: " + messageId);
            
            Assert.assertEquals(1,  scheduler.getMessageCount());
            
            // Read it by the messageId
            final Message m1rm = scheduler.peekMessage(messageId);
            System.out.println("m1rm: " + m1rm);
            Assert.assertNotNull(m1rm);
            
            // Read it by the key
            final Message m1rk = scheduler.peekMessageByKey(key);
            System.out.println("m1rk:" + m1rk);
            Assert.assertNotNull(m1rk);
            
            // Delete the message
            scheduler.deleteMessageByKey(key);
            
            // Read and verify that it is gone
            final Message m1rkd = scheduler.peekMessageByKey(key);
            Assert.assertNull(m1rkd);
            
            // Read and verify that it is gone
            final Message m1rmd = scheduler.peekMessage(messageId);
            Assert.assertNull(m1rmd);
        }
        
        {
            // Send another message
            final Message m = new Message().setUniqueKey(key);
            System.out.println("m2: " + m);
            final String messageId2 = producer.sendMessage(m);
            System.out.println("MessageId2: " + messageId2);
    
            try {
                final Message m2 = new Message().setUniqueKey(key);
                producer.sendMessage(m2);
                Assert.fail("Message should already exists");
            }
            catch (MessageQueueException e) {
                LOG.info("Failed to insert duplicate key", e);
            }
            
            try {
                List<Message> messages = Lists.newArrayList(
                    new Message().setUniqueKey(key), 
                    new Message().setUniqueKey(key2));
                
                SendMessageResponse result = producer.sendMessages(messages);
                Assert.assertEquals(1,  result.getMessages().size());
                Assert.assertEquals(1,  result.getNotUnique().size());
            }
            catch (MessageQueueException e) {
                Assert.fail(e.getMessage());
            }
            
            Map<String, Integer> counts = scheduler.getShardCounts();
            System.out.println(counts);
            Assert.assertEquals(2,  scheduler.getMessageCount());
            
            // Delete the message
            scheduler.deleteMessageByKey(key2);

            // Read the message
            final Collection<MessageContext> lm2 = consumer.readMessages(10, 10, TimeUnit.SECONDS);
            System.out.println("Read message: " + lm2);
            Assert.assertEquals(1,  lm2.size());
            System.out.println(lm2);
            Assert.assertEquals(1,  scheduler.getMessageCount());

            consumer.ackMessages(lm2);
            Assert.assertEquals(0,  scheduler.getMessageCount());
        }
        
        {
            final Message m = new Message()
                .setTrigger(new RepeatingTrigger.Builder()
                    .withInterval(3,  TimeUnit.SECONDS)
                    .withRepeatCount(10)
                    .build());
            final String messageId3 = producer.sendMessage(m);
            Assert.assertNotNull(messageId3);
            final Message m3rm = scheduler.peekMessage(messageId3);
            Assert.assertNotNull(m3rm);
            System.out.println(m3rm);
            Assert.assertEquals(1,  scheduler.getMessageCount());
            scheduler.deleteMessage(messageId3);
            Assert.assertEquals(0,  scheduler.getMessageCount());
        }
        
//        {
//            final String repeatingKey = "RepeatingMessage";
//            final Message m = new Message()
//                .setKey(repeatingKey)
//                .setKeepHistory(true)
//                .setTaskClass(HelloWorldFunction.class.getCanonicalName())
//                .setTrigger(new RepeatingTrigger.Builder()
//                    .withInterval(3,  TimeUnit.SECONDS)
//                    .withRepeatCount(5)
//                    .build());
//            final String messageId = producer.sendMessage(m);
//        
//            final AtomicLong counter = new AtomicLong(0);
//            
//            MessageQueueDispatcher dispatcher = new MessageQueueDispatcher.Builder()
//                .withBatchSize(5)
//                .withCallback(new Function<MessageContext, Boolean>() {
//                    long startTime = 0;
//                    
//                    @Override
//                    public synchronized Boolean apply(MessageContext message) {
//                        if (startTime == 0) 
//                            startTime = System.currentTimeMillis();
//                        
//                        System.out.println("Callback : " + (System.currentTimeMillis() - startTime) + " " + message);
//                        counter.incrementAndGet();
//                        return true;
//                    }
//                })
//                .withMessageQueue(scheduler)
//                .withThreadCount(2)
//                .build();
//            
//            dispatcher.start();
//            
//            Thread.sleep(TimeUnit.MILLISECONDS.convert(20,  TimeUnit.SECONDS));
//            
//            Collection<MessageHistory> history = scheduler.getKeyHistory(repeatingKey, null, null, 10);
//            System.out.println(history);
//            
//            dispatcher.stop();
//            
//            Assert.assertEquals(5,  counter.get());
//        }
        
        // Add a batch of messages and peek
        {
            List<Message> messages = Lists.newArrayList();
            
            for (int i = 0; i < 10; i++) {
                messages.add(new Message().addParameter("body", "" + i));
            }
            
            producer.sendMessages(messages);
            
            Collection<Message> all = consumer.peekMessages(Integer.MAX_VALUE);
            Assert.assertEquals(10,  all.size());
            
            for (Message msg : all) {
                System.out.println(msg.getParameters());
            }
        }
        
    }
    
    @Test
    @Ignore
    public void testStressQueue() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(100);
        
        final AtomicLong counter = new AtomicLong(0);
        final AtomicLong insertCount = new AtomicLong(0);
        final long max_count = 1000000;

        final CountingQueueStats stats = new CountingQueueStats();
        
        final ConsistencyLevel cl = ConsistencyLevel.CL_ONE;
        final MessageQueue scheduler = new ShardedDistributedMessageQueue.Builder()
            .withColumnFamily(SCHEDULER_NAME_CF_NAME)
            .withKeyspace(keyspace)
            .withConsistencyLevel(cl)
            .withStats(stats)
            .withTimeBuckets(10,  30,  TimeUnit.SECONDS)
            .withShardCount(100)
            .withPollInterval(100L,  TimeUnit.MILLISECONDS)
            .build();
        
        scheduler.createStorage();
        Thread.sleep(1000);
        scheduler.createQueue();
        
        final ConcurrentMap<String, Boolean> lookup = Maps.newConcurrentMap();
        
        final int batchSize = 50;
        
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                MessageProducer producer = scheduler.createProducer();
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
                                .build())
                            );
                    }
                    try {
                        producer.sendMessages(messages);
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
//                    MessageProducer producer = scheduler.createProducer();
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
//                                producer.sendMessages(tasks);
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
//                            producer.sendMessages(tasks);
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
//                    System.out.println("#### Processed : " + (newCount - prevCount.get()) + " of " + newCount + " (" + (insertCount.get() - newCount) + ")");
//                        System.out.println("#### Pending   : " + scheduler.getTaskCount());
//                        for (Entry<String, Integer> shard : producer.getShardCounts().entrySet()) {
//                            LOG.info("  " + shard.getKey() + " : " + shard.getValue());
//                        }
                    System.out.println(stats.toString());
                    System.out.println("" + (newCount - prevCount.get()) + " /sec  (" + newCount + ")");
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
//                            System.out.println("Total: " + total + " " + counts.toString());
//                        } catch (MessageQueueException e) {
//                            // TODO Auto-generated catch block
//                            e.printStackTrace();
//                        }
//                    }

                } 
                catch (Exception e) {
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

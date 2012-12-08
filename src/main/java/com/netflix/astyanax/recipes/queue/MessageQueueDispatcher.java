package com.netflix.astyanax.recipes.queue;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.netflix.astyanax.recipes.locks.BusyLockException;

/**
 * The message queue dispatcher reads message from the message queue
 * and dispatches to worker threads.
 * 
 * @author elandau
 *
 */
public class MessageQueueDispatcher {
    private static final Logger LOG = LoggerFactory.getLogger(MessageQueueDispatcher.class);
    
    public final static int   DEFAULT_BATCH_SIZE            = 5;
    public final static int   DEFAULT_POLLING_INTERVAL      = 1; // Seconds
    public final static int   THROTTLE_DURATION             = 1000;
    public final static int   DEFAULT_THREAD_COUNT          = 1;
    public final static int   DEFAULT_CONSUMER_COUNT        = 1;
    
    public static class Builder {
        private final MessageQueueDispatcher dispatcher = new MessageQueueDispatcher();
        
        /**
         * Specify the message queue to use for this dispatcher
         * @param messageQueue
         * @return
         */
        public Builder withMessageQueue(MessageQueue messageQueue) {
            dispatcher.messageQueue = messageQueue;
            return this;
        }
        
        /**
         * Change the number of threads reading from the queue
         * 
         * @param threadCount
         */
        public Builder withThreadCount(int threadCount) {
            dispatcher.threadCount = threadCount;
            return this;
        }
        
        public Builder withConsumerCount(int consumerCount) {
            dispatcher.consumerCount = consumerCount;
            return this;
        }
        
        /**
         * Use this external executor
         * @param executor
         * @return
         */
        public Builder withExecutor(ExecutorService executor) {
            dispatcher.executor = executor;
            return this;
        }
        
        /**
         * Number of 'triggers' to read from the queue in each call.  
         * Default is 1
         * @param batchSize
         */
        public Builder withBatchSize(int batchSize) {
            dispatcher.batchSize = batchSize;
            return this;
        }
        
        public Builder withCallback(Function<Message, Boolean> callback) {
            dispatcher.callback = callback;
            return this;
        }
        
        public MessageQueueDispatcher build() {
            dispatcher.initialize();
            return dispatcher;
        }
    }
    
    private int             threadCount   = DEFAULT_THREAD_COUNT;
    private int             batchSize     = DEFAULT_BATCH_SIZE;
    private int             consumerCount = DEFAULT_CONSUMER_COUNT;
    private boolean         terminate     = true;
    private MessageQueue    messageQueue;
    private ExecutorService executor;
    private boolean         bOwnedExecutor = false;
    private Function<Message, Boolean>   callback;
    private LinkedBlockingQueue<Message> toAck = Queues.newLinkedBlockingQueue();

    private MessageQueueDispatcher() {
    }
    
    private void initialize() {
        Preconditions.checkNotNull(messageQueue, "Must specify message queue");
    }
    
    public void start() {
        if (executor == null) {
            executor = Executors.newFixedThreadPool(threadCount);
            bOwnedExecutor = true;
        }
        
        for (int i = 0; i < consumerCount; i++) {
            startConsumer(i);
        }
    }
    
    public void stop() {
        terminate = true;
        if (bOwnedExecutor) 
            executor.shutdownNow();
    }
    
    private void enqueMessage(final Message message) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    if (callback.apply(message)) {
                        executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                // TODO Auto-generated method stub
                                
                            }
                        });
                        toAck.add(message);
                    }
                }
                catch (Throwable t) {
                    LOG.error("Error processing message");
                }
            }
        });
    }
    
    private void startConsumer(final int id) {
        final String name = StringUtils.join(Lists.newArrayList(messageQueue.getName(), "Consumer", Integer.toString(id)), ":");
        
        executor.submit(new Runnable() {
            @Override
            public void run() {
                if (terminate == true)
                    return;
                
                Thread.currentThread().setName(name);
                
                // Create the consumer context
                MessageConsumer consumer = messageQueue.createConsumer();
                
                // Process events in a tight loop, until asked to terminate
                Collection<Message> messages = null;
                try {
                    messages = consumer.readMessages(batchSize);
                    for (Message message : messages) {
                        enqueMessage(message);
                    }
                    consumer.ackMessages(toAck);
                } 
                catch (BusyLockException e) {
                    try {
                        Thread.sleep(THROTTLE_DURATION);
                    } catch (InterruptedException e1) {
                    }
                }
                catch (Exception e) {
                    LOG.warn("Error consuming messages ", e);
                    try {
                        Thread.sleep(THROTTLE_DURATION);
                    } catch (InterruptedException e1) {
                    }
                }
                
                if (!terminate) {
                    executor.submit(this);
                }
            }
        });
    }
}

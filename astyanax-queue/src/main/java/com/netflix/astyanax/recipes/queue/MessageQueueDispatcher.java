package com.netflix.astyanax.recipes.queue;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

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
    public final static int   DEFAULT_POLLING_INTERVAL     = 1000;
    public final static int   DEFAULT_THREAD_COUNT          = 1;
    public final static int   DEFAULT_CONSUMER_COUNT        = 1;
    public final static int   DEFAULT_ACK_SIZE              = 100;
    public final static int   DEFAULT_ACK_INTERVAL          = 100;
    public final static int   DEFAULT_BACKLOG_SIZE          = 1000;
    
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
         * @deprecated Use withProcessorThreadCount
         */
        public Builder withThreadCount(int threadCount) {
            return withProcessorThreadCount(threadCount);
        }
        
        /**
         * Specify number of threads that are processing events popped from the queue
         * @param threadCount
         * @return
         */
        public Builder withProcessorThreadCount(int threadCount) {
            dispatcher.processorThreadCount = threadCount;
            return this;
        }
        
        /**
         * Number of pending events to process in the backlog
         * @param size
         * @return
         */
        public Builder withBacklogSize(int size) {
            dispatcher.backlogSize = size;
            return this;
        }
        
        /**
         * Set the number of consumers that will be removing items from the 
         * queue.  This value must be less than or equal to the thread count.
         * @param consumerCount
         * @return
         */
        public Builder withConsumerCount(int consumerCount) {
            dispatcher.consumerCount = consumerCount;
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
        
        /**
         * Flush the ack queue on this interval.
         * @param interval
         * @param units
         */
        public Builder withAckInterval(long interval, TimeUnit units) {
            dispatcher.ackInterval = TimeUnit.MILLISECONDS.convert(interval, units);
            return this;
        }
        
        /**
         * Interval for polling from the queue.  
         * @param interval
         * @param units
         */
        public Builder withPollingInterval(long interval, TimeUnit units) {
            dispatcher.pollingInterval = TimeUnit.MILLISECONDS.convert(interval, units);
            return this;
        }
        
        /**
         * Callback to process messages.  The callback is called from any of the internal processing
         * threads and is therefore not thread safe.
         * @param callback
         * @return true to ack the message, false to not ack and cause the message to timeout
         *          Throw an exception to force the message to be added to the poison queue
         */
        public Builder withCallback(Function<MessageContext, Boolean> callback) {
            dispatcher.callback = callback;
            return this;
        }
        
        /**
         * Provide a message handler factory to use when creating tasks.
         * @param factory
         * @return
         */
        public Builder withMessageHandlerFactory(MessageHandlerFactory factory) {
            dispatcher.handlerFactory = factory;
            return this;
        }
        
        public MessageQueueDispatcher build() {
            Preconditions.checkArgument(dispatcher.consumerCount <= dispatcher.processorThreadCount, "consumerCounter must be <= threadCount");
            dispatcher.initialize();
            return dispatcher;
        }
    }
    
    private int             processorThreadCount   = DEFAULT_THREAD_COUNT;
    private int             batchSize     = DEFAULT_BATCH_SIZE;
    private int             consumerCount = DEFAULT_CONSUMER_COUNT;
    private int             ackSize       = DEFAULT_ACK_SIZE;
    private long            ackInterval   = DEFAULT_ACK_INTERVAL;
    private int             backlogSize   = DEFAULT_BACKLOG_SIZE;
    private long            pollingInterval = DEFAULT_POLLING_INTERVAL;
    private boolean         terminate     = false;
    private MessageQueue    messageQueue;
    private ExecutorService executor;
    private MessageConsumer ackConsumer;
    private Function<MessageContext, Boolean>   callback;
    private MessageHandlerFactory handlerFactory;
    private LinkedBlockingQueue<MessageContext> toAck = Queues.newLinkedBlockingQueue();
    private LinkedBlockingQueue<MessageContext> toProcess = Queues.newLinkedBlockingQueue(500);
    
    private MessageQueueDispatcher() {
    }
    
    private void initialize() {
        Preconditions.checkNotNull(messageQueue, "Must specify message queue");
        
        if (this.handlerFactory == null)
            this.handlerFactory = new SimpleMessageHandlerFactory();
        toProcess = Queues.newLinkedBlockingQueue(backlogSize);
    }
    
    public void start() {
        executor = Executors.newScheduledThreadPool(processorThreadCount + consumerCount + 1);
        
        startAckThread();
        
        for (int i = 0; i < consumerCount; i++) {
            startConsumer(i);
        }
        
        for (int i = 0; i < processorThreadCount; i++) {
            startProcessor(i);
        }
    }
    
    public void stop() {
        terminate = true;
        executor.shutdownNow();
    }
    
    private void startAckThread() {
        ackConsumer = messageQueue.createConsumer();
        
        executor.submit(new Runnable() {
            @Override
            public void run() {
                String name = StringUtils.join(Lists.newArrayList(messageQueue.getName(), "Ack"), ":");
                Thread.currentThread().setName(name);
                
                while (!terminate) {
                    try {
                        List<MessageContext> messages = Lists.newArrayList();
                        toAck.drainTo(messages);
                        if (!messages.isEmpty()) {
                            try {
                                ackConsumer.ackMessages(messages);
                            } catch (MessageQueueException e) {
                                toAck.addAll(messages);
                                LOG.warn("Failed to ack consumer", e);
                            }
                        }
                    }
                    catch (Throwable t) {
                        LOG.info("Error acking messages", t);
                    }
                    
                    try {
                        Thread.sleep(ackInterval);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        });
    }
    
    private void startConsumer(final int id) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                String name = StringUtils.join(Lists.newArrayList(messageQueue.getName(), "Consumer", Integer.toString(id)), ":");
                Thread.currentThread().setName(name);
                
                // Create the consumer context
                final MessageConsumer consumer = messageQueue.createConsumer();
                
                while (!terminate) {
                    // Process events in a tight loop, until asked to terminate
                    Collection<MessageContext> messages = null;
                    try {
                        messages = consumer.readMessages(batchSize);
                        if (messages.isEmpty()) {
                            Thread.sleep(pollingInterval);
                        }
                        else {
                            for (MessageContext context : messages) {
                                toProcess.put(context);
                            }
                        }
                    } 
                    catch (BusyLockException e) {
                        try {
                            Thread.sleep(pollingInterval);
                        } catch (InterruptedException e1) {
                            Thread.interrupted();
                            return;
                        }
                    }
                    catch (Throwable t) {
                        LOG.warn("Error consuming messages ", t);
                    }
                }
            }
        });
    }
    
    private void startProcessor(final int id) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                String name = StringUtils.join(Lists.newArrayList(messageQueue.getName(), "Processor", Integer.toString(id)), ":");
                Thread.currentThread().setName(name);
                LOG.info("Starting message processor : " + name);
                try {
                    while (!terminate) {
                        // Pop a message off the queue, blocking if empty
                        final MessageContext context;
                        try {
                            context = toProcess.take();
                            if (context == null)
                                continue;
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        
                        // Process the message
                        Message message = context.getMessage();
                        try {
                            // Message has a specific handler class
                            if (message.getTaskClass() != null) {
                                @SuppressWarnings("unchecked")
                                Function<MessageContext, Boolean> task = handlerFactory.createInstance(message.getTaskClass());
                                if (task.apply(context)) {
                                    toAck.add(context);
                                }
                                continue;
                            }
                            
                            // Use default callback
                            if (callback.apply(context)) {
                                context.setStatus(MessageStatus.DONE);
                                toAck.add(context);
                                continue;
                            }
                        }
                        catch (Throwable t) {
                            context.setException(t);
                            toAck.add(context);
                            LOG.error("Error processing message " + message.getKey(), t);
//                            try {
//                                ackConsumer.ackPoisonMessage(context);
//                            } catch (MessageQueueException e) {
//                                LOG.warn("Failed to ack poison message " + message.getKey(), e);
//                            }
                        }
                    }
                }
                catch (Throwable t) {
                    LOG.error("Error running producer : " + name, t);
                }
            }
        });
    }
}

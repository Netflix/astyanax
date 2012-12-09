package com.netflix.astyanax.recipes.scheduler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageConsumer;
import com.netflix.astyanax.recipes.queue.MessageProducer;
import com.netflix.astyanax.recipes.queue.MessageQueueException;
import com.netflix.astyanax.recipes.queue.MessageQueueHooks;
import com.netflix.astyanax.recipes.queue.ShardedDistributedMessageQueue;
import com.netflix.astyanax.recipes.uniqueness.ColumnPrefixUniquenessConstraint;
import com.netflix.astyanax.recipes.uniqueness.NotUniqueException;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import com.netflix.astyanax.util.TimeUUIDUtils;

/**
 * Implementation of a task scheduler on top of the MessageQueue recipe.
 * 
 * @author elandau
 *
 * TODO:  Execute the tasks in a separate executor
 */
public class DistributedTaskScheduler implements MessageQueueHooks, TaskScheduler {
    private final static Logger LOG = LoggerFactory.getLogger(DistributedTaskScheduler.class);
    
    public final static String           DEFAULT_SCHEDULER_NAME        = "Tasks";
    public final static ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL     = ConsistencyLevel.CL_QUORUM;
    public final static int              DEFAULT_BATCH_SIZE            = 5;
    public final static int              DEFAULT_CONSUMER_THREAD_COUNT = 1;
    public final static Integer          DEFAULT_ERROR_TTL             = (int)TimeUnit.SECONDS.convert(14,  TimeUnit.DAYS);
    public final static int              DEFAULT_SCHEMA_AGREEMENT_GRACE_PERIOD = 3000;
    public final static int              DEFAULT_SHARD_COUNT           = 1;
    public final static int              DEFAULT_BUCKET_COUNT          = 1;
    public final static int              DEFAULT_BUCKET_DURATION       = 1;
    public final static int              DEFAULT_POLLING_INTERVAL      = 1; // Seconds
    public final static int              THROTTLE_DURATION             = 1000;
    
    public final static String           TASKS_SUFFIX        = "_Tasks";
    public final static String           QUEUE_SUFFIX        = "_Queue";
    public final static String           HISTORY_SUFFIX      = "_History";
    
    public final static String           COLUMN_TASK_INFO     = "task_info";
    public final static String           COLUMN_TRIGGER_CLASS = "trigger_class";
    public final static String           COLUMN_TRIGGER       = "trigger";
    public final static String           COLUMN_TOKEN         = "token";
    public final static String           COLUMN_STATE         = "state";
    public final static String           PARAM_KEY            = "key";

    private final ObjectMapper mapper;
    
    {
        mapper = new ObjectMapper();
        mapper.configure(SerializationConfig.Feature.USE_STATIC_TYPING, true);
    }

    /**
     * Builder for the scheduler
     * 
     * @author elandau
     */
    public static class Builder {
        private DistributedTaskScheduler taskExecutor = new DistributedTaskScheduler();
        
        /**
         * You should only really call this if you want to change the consistency level 
         * to CL_QUORUM_EACH for multi-dc deploayment, which wouldn't make much sense
         * for a distributed scheduler due to the increased locking latency.
         * 
         * @param consistencyLevel
         */
        public Builder withConsistencyLevel(ConsistencyLevel consistencyLevel) {
            taskExecutor.consistencyLevel = consistencyLevel;
            return this;
        }
        
        /**
         * Change the number of threads reading from the queue
         * 
         * @param threadCount
         */
        public Builder withThreadCount(int threadCount) {
            taskExecutor.threadCount = threadCount;
            return this;
        }
        
        /**
         * Number of 'triggers' to read from the queue in each call.  
         * Default is 1
         * @param batchSize
         */
        public Builder withBatchSize(int batchSize) {
            taskExecutor.batchSize = batchSize;
            return this;
        }
        
        /**
         * Unique name given to the scheduler.  The scheduler uses 3 column families 
         * with the following naming convension
         * 
         * {name}_Tasks
         * {name}_History
         * {name}_Queue
         * 
         * @param schedulerName
         */
        public Builder withName(String schedulerName) {
            taskExecutor.name = schedulerName;
            return this;
        }
        
        /**
         * Group name within the column families.  This allows for multiple schedulers
         * to run in the same column famil.
         * @param schedulerName
         * @return
         */
        public Builder withGroupName(String groupName) {
            taskExecutor.groupName = groupName;
            return this;
        }
        
        /**
         * The keyspace client to use 
         * @param keyspace
         */
        public Builder withKeyspace(Keyspace keyspace) {
            taskExecutor.keyspace = keyspace;
            return this;
        }
        
        /**
         * Set the polling interval for checking for events to execute.
         * 
         * @param interval
         * @param units     Lowest granularity is in seconds
         * @return
         */
        public Builder withPollingInterval(long interval, TimeUnit units) {
            taskExecutor.pollingInterval = TimeUnit.SECONDS.convert(interval, units);
            return this;
        }
        
        public TaskScheduler build() {
            taskExecutor.intialize();
            return taskExecutor;
        }
    }
    
    private ShardedDistributedMessageQueue  messageQueue;
    private ExecutorService     executor;
    private int                 threadCount      = DEFAULT_CONSUMER_THREAD_COUNT;
    private int                 batchSize        = DEFAULT_BATCH_SIZE;
    private volatile boolean    terminate        = false;
    private ConsistencyLevel    consistencyLevel = DEFAULT_CONSISTENCY_LEVEL;
    private String              name             = DEFAULT_SCHEDULER_NAME;
    private String              groupName;
    private MessageProducer     producer;
    private Keyspace            keyspace;
    private ColumnFamily<String, String> taskCf;
    private ColumnFamily<String, UUID>   historyCf;
    private long                pollingInterval = DEFAULT_POLLING_INTERVAL;
    
    public DistributedTaskScheduler() {
        
    }
    
    /**
     * Perform initialization after the scheduler is built by the Builder
     */
    private void intialize() {
        Preconditions.checkNotNull(keyspace, "Must specify keyspace");
        
        taskCf    = new ColumnFamily<String, String>(name + TASKS_SUFFIX,   StringSerializer.get(), StringSerializer.get());
        historyCf = new ColumnFamily<String, UUID>  (name + HISTORY_SUFFIX, StringSerializer.get(), TimeUUIDSerializer.get());
        
        if (groupName == null) 
            groupName = name;
        
        executor = Executors.newFixedThreadPool(threadCount);
        
        messageQueue = new ShardedDistributedMessageQueue.Builder()
                    .withColumnFamily(taskCf.getName() + QUEUE_SUFFIX)
                    .withQueueName(groupName)
                    .withKeyspace(keyspace)
                    .withConsistencyLevel(consistencyLevel)
                    .withShardCount(DEFAULT_SHARD_COUNT)
                    .withHook(this)
                    .withBuckets(DEFAULT_BUCKET_COUNT,  DEFAULT_BUCKET_DURATION,  TimeUnit.HOURS)
                    .withPollInterval(pollingInterval,  TimeUnit.SECONDS)
                    .build();
        producer = messageQueue.createProducer();
        
    }
    
    /* (non-Javadoc)
     * @see com.netflix.astyanax.recipes.scheduler.TaskScheduler#create()
     */
    @Override
    public void create() throws TaskSchedulerException {
        // Create the message queue and wait for the schema agreement to propograte
        try {
            messageQueue.createQueue();
        } catch (MessageQueueException e) {
            // TODO: Ignore if already exists
            throw new TaskSchedulerException("Failed to create message queue for scheduler " + taskCf.getName(), e);
        }
        try {
            Thread.sleep(DEFAULT_SCHEMA_AGREEMENT_GRACE_PERIOD);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
        }
        
        // Create the Task column family and wait for the schema agreement to propogate
        try {
            keyspace.createColumnFamily(taskCf, ImmutableMap.<String, Object>builder()
                    .put("column_metadata", ImmutableMap.<String, Object>builder()
                            .put(COLUMN_TASK_INFO, ImmutableMap.<String, Object>builder()
                                    .put("validation_class", "UTF8Type")
                                    .build())
                            .put(COLUMN_TRIGGER_CLASS, ImmutableMap.<String, Object>builder()
                                    .put("validation_class", "UTF8Type")
                                    .build())
                            .put(COLUMN_TRIGGER, ImmutableMap.<String, Object>builder()
                                    .put("validation_class", "UTF8Type")
                                    .build())
                            .put(COLUMN_STATE, ImmutableMap.<String, Object>builder()
                                    .put("validation_class", "UTF8Type")
                                    .build())
                            .put(COLUMN_TOKEN, ImmutableMap.<String, Object>builder()
                                    .put("validation_class", "UUIDType")
                                    .build())
                            .build())
                         .build());
        } catch (ConnectionException e) {
            // TODO: Ignore if already exists
            throw new TaskSchedulerException("Failed to create column family for scheduler " + taskCf.getName(), e);
        }
        try {
            Thread.sleep(DEFAULT_SCHEMA_AGREEMENT_GRACE_PERIOD);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
        }
        
        // Create the history column family and wait for it to propogate
        try {
            keyspace.createColumnFamily(historyCf, ImmutableMap.<String, Object>builder()
                    .put("default_validation_class", "UTF8Type")
                    .build());
        } catch (ConnectionException e) {
            throw new TaskSchedulerException("Failed to create column family for scheduler history " + historyCf.getName(), e);
        }
        try {
            Thread.sleep(DEFAULT_SCHEMA_AGREEMENT_GRACE_PERIOD);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
        }
    }
    
    /* (non-Javadoc)
     * @see com.netflix.astyanax.recipes.scheduler.TaskScheduler#start()
     */
    @Override
    public void start() {
        for (int i = 0; i < threadCount; i++) {
            startConsumer(i);
        }
    }
    
    /* (non-Javadoc)
     * @see com.netflix.astyanax.recipes.scheduler.TaskScheduler#stop()
     */
    @Override
    public boolean stop() throws TaskSchedulerException {
        terminate = true;
        executor.shutdownNow();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }
    
    /**
     * Start a consumer thread to process events
     * @param id
     */
    private void startConsumer(final int id) {
        executor.submit(new Runnable() {
            private String name;
            
            @Override
            public void run() {
                // Give it a name
                name = StringUtils.join(Lists.newArrayList(messageQueue.getName(), "Consumer", Integer.toString(id)), ":");
                Thread.currentThread().setName(name);
                
                // Create the consumer context
                MessageConsumer consumer = messageQueue.createConsumer();
                try {
                    // Process events in a tight loop, until asked to terminate
                    while (!terminate) {
                        Collection<Message> messages = null;
                        try {
                            messages = consumer.readMessages(batchSize);
                            try {
                                for (Message message : messages) {
                                    
                                    TaskInfo            info    = null;
                                    String              taskKey = null;
                                    ColumnList<String>  columns = null;
                                    
                                    TaskHistory history = new TaskHistory();
                                    history.setStartTime(System.currentTimeMillis());
                                    history.setTriggerTime(message.getTrigger().getTriggerTime());
                                    
                                    UUID historyUUID = TimeUUIDUtils.getTimeUUID(history.getStartTime());
                                    
                                    try {
                                        // Get the key from the message parameters
                                        Map<String, Object> parameters = message.getParameters();
                                        taskKey = (String)parameters.get(PARAM_KEY);
                                        // Get all column data for the task
                                        columns = keyspace.prepareQuery(taskCf)
                                                 .getRow(taskKey)
                                                 .execute()
                                                 .getResult();
                                        // TODO: Check that the token isn't stale
                                        if (columns.isEmpty()) {
                                            // Task was deleted so just ignore it
                                            continue;
                                        }
                                        String triggerClassName = columns.getStringValue(COLUMN_TRIGGER_CLASS, null);
                                        // String triggerData      = columns.getStringValue(COLUMN_TRIGGER,       null);
                                        // TODO: Compare triggers for updates
                                        String triggerData      = (String)message.getParameters().get(COLUMN_TRIGGER);
                                        TaskState state         = TaskState.valueOf(columns.getStringValue(COLUMN_STATE, TaskState.Active.name()));
                                        
                                        // If inactive then we simply skip processing the trigger
                                        info = deserializeString(columns.getStringValue(COLUMN_TASK_INFO, null), TaskInfo.class);
                                        if (state == TaskState.Inactive) 
                                            history.setStatus(TaskStatus.SKIPPED);
                                        else
                                            history.setStatus(TaskStatus.RUNNING);
                                        
                                        // Log that the task is starting processing.  The same column will be updated
                                        // when processing is done
                                        if (info.isKeepHistory()) {
                                            keyspace.prepareColumnMutation(historyCf, taskKey, historyUUID)
                                                .putValue(serializeToString(history), info.getHistoryTtl())
                                                .execute();
                                        }
                                        
                                        // Resubmit the trigger if it is a repeating trigger
                                        Task task = (Task) Class.forName(info.getClassName()).newInstance();
                                        Trigger trigger = null;
                                        if (triggerClassName != null) {
                                            trigger = deserializeString(triggerData, triggerClassName);
                                            Trigger nextTrigger = trigger.nextTrigger();
                                            if (nextTrigger != null) {
                                                sendMessage(taskKey, nextTrigger);
                                            }
                                        }
                                        
                                        // Execute the task
                                        if (state == TaskState.Active) {
                                            task.execute(info);
                                            history.setStatus(TaskStatus.DONE);
                                        }
                                    }
                                    catch (Throwable t) {
                                        // On any exception 
                                        LOG.warn("Error executing task " + taskKey, t);
                                        history.setError(t.getMessage());
                                        history.setStackTrace(ExceptionUtils.getStackTrace(t));
                                        history.setStatus(TaskStatus.FAILED);
                                    }
                                    finally {
                                        // Track processing history
                                        if (columns != null && taskKey != null && (info == null || info.isKeepHistory())) {
                                            try {
                                                history.setEndTime(System.currentTimeMillis());
                                                String value = serializeToString(history);
                                                Integer ttl = DEFAULT_ERROR_TTL;
                                                if (info != null)
                                                    ttl = info.getHistoryTtl();
                                                keyspace.prepareColumnMutation(historyCf, taskKey, historyUUID)
                                                    .putValue(value, ttl)
                                                    .execute();
                                            }
                                            catch (Exception e) {
                                                LOG.warn("Error saving history for " + taskKey, e);
                                            }
                                        }
                                    }
                                }
                            }
                            finally {
                                consumer.ackMessages(messages);
                            }
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
                    }
                }
                finally {
                    LOG.info("Done with consumer " + name);
                }
            }
        });
    }
    
    /* (non-Javadoc)
     * @see com.netflix.astyanax.recipes.scheduler.TaskScheduler#scheduleTask(com.netflix.astyanax.recipes.scheduler.TaskInfo, com.netflix.astyanax.recipes.scheduler.Trigger)
     */
    @Override
    public void scheduleTask(final TaskInfo task, final Trigger trigger) throws TaskSchedulerException, NotUniqueException {
        final String rowKey = getGroupKey(task.getKey());
        
        ColumnPrefixUniquenessConstraint<String> unique = new ColumnPrefixUniquenessConstraint<String>(keyspace, taskCf, rowKey)
                .withConsistencyLevel(consistencyLevel);
        
        final String serializedTask;
        final String serializedTrigger;
        try {
            serializedTask    = serializeToString(task);
            serializedTrigger = serializeToString(trigger);
        } catch (Exception e) {
            throw new TaskSchedulerException("Failed to serialize trigger or task for " + rowKey, e);
        }

        try {
            unique.acquireAndApplyMutation(new Function<MutationBatch, Boolean>() {
                @Override
                public Boolean apply(@Nullable MutationBatch mb) {
                    mb.withRow(taskCf, rowKey)
                        .putColumn(COLUMN_TASK_INFO,     serializedTask)
                        .putColumn(COLUMN_TRIGGER,       serializedTrigger)
                        .putColumn(COLUMN_TRIGGER_CLASS, trigger.getClass().getCanonicalName());
                    return true;
                }
            });
        } catch (Exception e) {
            throw new TaskSchedulerException("Failed to serialize trigger or task for " + rowKey, e);
        }
        
        // Now, send 
        try {
            sendMessage(rowKey, trigger);
        } catch (Exception e) {
            throw new TaskSchedulerException("Failed to send message for task " + rowKey, e);
        }
    }
    
    /* (non-Javadoc)
     * @see com.netflix.astyanax.recipes.scheduler.TaskScheduler#stopTask(java.lang.String)
     */
    @Override
    public void stopTask(String taskKey) throws TaskSchedulerException {
        String rowKey = getGroupKey(taskKey);
        
        try {
            keyspace.prepareColumnMutation(taskCf, rowKey, COLUMN_STATE)
                .putValue(TaskState.Inactive.name(), null)
                .execute();
        } catch (ConnectionException e) {
            throw new TaskSchedulerException("Failed to deactive task " + rowKey, e);
        }
    }
    
    /* (non-Javadoc)
     * @see com.netflix.astyanax.recipes.scheduler.TaskScheduler#startTask(java.lang.String)
     */
    @Override
    public void startTask(String taskKey) throws TaskSchedulerException {
        String rowKey = getGroupKey(taskKey);
        
        try {
            keyspace.prepareColumnMutation(taskCf, rowKey, COLUMN_STATE)
                .putValue(TaskState.Active.name(), null)
                .execute();
        } catch (ConnectionException e) {
            throw new TaskSchedulerException("Failed to deactive task " + rowKey, e);
        }
    }
    
    /* (non-Javadoc)
     * @see com.netflix.astyanax.recipes.scheduler.TaskScheduler#deleteTask(java.lang.String)
     */
    @Override
    public void deleteTask(String taskKey) throws TaskSchedulerException {
        String rowKey = getGroupKey(taskKey);
        
        MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
        mb.withRow(taskCf, rowKey).delete();
        try {
            mb.execute();
        } catch (ConnectionException e) {
            throw new TaskSchedulerException("Failed to delete task " + rowKey, e);
        }
    }
    
    /* (non-Javadoc)
     * @see com.netflix.astyanax.recipes.scheduler.TaskScheduler#getTaskHistory(java.lang.String, java.lang.Long, java.lang.Long)
     */
    @Override
    public Collection<TaskHistory> getTaskHistory(String taskKey, Long timeFrom, Long timeTo, int count) throws TaskSchedulerException {
        return null;
    }

    /**
     * Send a message to the queue.  The message contains references to the actual task recoed.
     * @param taskKey
     * @param trigger
     * @return Message that was sent.  This includes the token representing the column in the message queue.
     * @throws Exception
     */
    private Message sendMessage(String taskKey, Trigger trigger) throws Exception {
        Message message = new Message();
        message.setParameters(ImmutableMap.<String, Object>builder()
                .put(PARAM_KEY,         taskKey)
                .put(COLUMN_TRIGGER,    serializeToString(trigger))
                .build());
// TODO:     message.getTrigger().setTriggerTime(trigger.getTriggerTime());
        
        producer.sendMessage(message);
        return message;
    }
    
    private <T> String serializeToString(T trigger) throws JsonGenerationException, JsonMappingException, IOException  {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mapper.writeValue(baos, trigger);
        baos.flush();
        return baos.toString();
    }
    
    private <T> T deserializeString(String data, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
        return (T) mapper.readValue(
                new ByteArrayInputStream(data.getBytes()), 
                clazz);
    }
    
    private <T> T deserializeString(String data, String className) throws JsonParseException, JsonMappingException, IOException, ClassNotFoundException {
        return (T) mapper.readValue(
                new ByteArrayInputStream(data.getBytes()), 
                Class.forName(className));
    }

    
    @Override
    public void beforeAckMessages(Collection<Message> messages, MutationBatch mb) {
    }

    @Override
    public void beforeAckMessage(Message message, MutationBatch mb) {
    }

    @Override
    public void beforeSendMessage(Message message, MutationBatch mb) {
        // Update the queued token in the task so it can be used to cancel the task 
        // execution later
        try {
            mb.withRow(taskCf, (String)message.getParameters().get(PARAM_KEY))
                .putColumn(COLUMN_TOKEN, message.getToken());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private String getGroupKey(String key) {
        return groupName + "$" + key;
    }
}

package com.netflix.astyanax.recipes.scheduler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Equality;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.TimeUUIDUtils;

/**
 * Algorithm:
 * 
 *  Jobs are stored as columns in an index where the columns are stored in time order.  The time can
 *  be the current time for immediate execution or future time for recurring or scheduled tasks.
 *  Jobs will be processed in time order.
 *  
 *  To achieve higher scalability the job queue (implemented as a row) is sharded by a user provided shard.
 *  Rows also implement a rolling time window which is used to alleviate tombstone pressure 
 * 
 * Enque:
 *  
 * Deque:
 *  1.  Lock + read top N columns
 *  2.  Select M jobs to process
 *      Select jobs in <state> = scheduled
 *      If any jobs are marked as processing then delete and update their state
 *  3.  Release the lock with a mutation that has a 
 *      delete for the columns being processed and 
 *      insert with the same data but <state> = processing
 *  4.  Process the jobs
 *  5.  If the processing thread is about to enter a section which is not repeatable then update the column 
 *      by changing the state to NotRepeatable.
 *  6.  Issue a delete for processed job
 *  
 * Schema:
 *      RowKey: TimeBucket + Shard
 *      Column: <type><priority><timeuuid><state>
 *      Value:  Job Data
 *      
 *      <type>
 *          0 - Lock meta
 *          1 - Queue item
 *      <state>
 *          0 - Lock columns - There are special columns that are used to lock the row
 *          1 - Scheduled
 *          2 - Processing - timeuuid = timeout
 *          3 - NotRepeatable - special indicator that tells the queue that the job is not replayble since there could
 *                          be a persistence 
 *          
 *    
 * Recurring Tasks:
 * 
 * @author elandau
 *
 */
public class ShardedDistributedScheduler implements TaskScheduler {
    public static final String           DEFAULT_COLUMN_FAMILY_NAME      = "Scheduler";
    public static final String           DEFAULT_QUEUE_NAME              = "Queue";
    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL       = ConsistencyLevel.CL_LOCAL_QUORUM;
    public static final RetryPolicy      DEFAULT_RETRY_POLICY            = RunOnce.get();
    public static final long             DEFAULT_LOCK_TIMEOUT            = TimeUnit.MICROSECONDS.convert(1,   TimeUnit.MINUTES);
    public static final long             DEFAULT_LOCK_TTL                = TimeUnit.MICROSECONDS.convert(10,  TimeUnit.MINUTES);
    public static final long             DEFAULT_POLL_WAIT               = TimeUnit.MILLISECONDS.convert(50,  TimeUnit.MILLISECONDS);
    public static final long             DEFAULT_VISIBILITY_TIMEOUT      = TimeUnit.SECONDS.convert(4,  TimeUnit.DAYS);
    public static final int              DEFAULT_SHARD_COUNT             = 1;
    public static final long             DEFAULT_BUCKET_DURATION         = TimeUnit.MICROSECONDS.convert(30,  TimeUnit.SECONDS);
    public static final int              DEFAULT_BUCKET_COUNT            = 1;
    
    private final ObjectMapper mapper;
    
    {
        mapper = new ObjectMapper();
        mapper.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    }

    public static class Builder {
        private ShardedDistributedScheduler scheduler        = new ShardedDistributedScheduler();
        private String                      columnFamilyName = DEFAULT_COLUMN_FAMILY_NAME;

        public Builder withColumnFamily(String columnFamilyName) {
            this.columnFamilyName = columnFamilyName;
            return this;
        }
        
        public Builder withShardCount(int count) {
            scheduler.settings.setShardCount(count);
            return this;
        }
        
        public Builder withBuckets(int bucketCount, int bucketDuration, TimeUnit units) {
            scheduler.settings.setPartitionDuration(TimeUnit.MICROSECONDS.convert(bucketDuration,  units));
            scheduler.settings.setPartitionCount(bucketCount);
            return this;
        }
        
        public Builder withVisibilityTimeout(Long timeout, TimeUnit units) {
            scheduler.settings.setVisibilityTimeout(TimeUnit.SECONDS.convert(timeout,  units));
            return this;
        }
        
        public Builder withLockTimeout(Long timeout, TimeUnit units) {
            scheduler.lockTimeout = TimeUnit.MICROSECONDS.convert(timeout,  units);
            return this;
        }
        
        public Builder withLockTtl(Long ttl, TimeUnit units) {
            scheduler.lockTtl = TimeUnit.SECONDS.convert(ttl,  units);
            return this;
        }
        
        public Builder withPollInterval(Long internval, TimeUnit units) {
            scheduler.pollInterval = TimeUnit.MILLISECONDS.convert(internval,  units);
            return this;
        }
        
        public Builder withQueueName(String queueName) {
            scheduler.queueName = queueName;
            return this;
        }
        
        public Builder withConsistencyLevel(ConsistencyLevel level) {
            scheduler.consistencyLevel = level;
            return this;
        }
        
        public Builder withKeyspace(Keyspace keyspace) {
            scheduler.keyspace = keyspace;
            return this;
        }
        
        public Builder withSchedulerStats(SchedulerStats stats) {
            scheduler.stats = stats;
            return this;
        }
        
        public Builder withHooks(SchedulerHooks hooks) {
            scheduler.hooks = hooks;
            return this;
        }
        
        public TaskScheduler build() {
            scheduler.columnFamily = ColumnFamily.newColumnFamily(columnFamilyName, StringSerializer.get(), compositeSerializer); 
            scheduler.initialize();
            return scheduler;
        }
    }

    private final static AnnotatedCompositeSerializer<SchedulerEntry> compositeSerializer = new AnnotatedCompositeSerializer<SchedulerEntry>(SchedulerEntry.class);
    
    // Immutable after configuration
    private ColumnFamily<String, SchedulerEntry> columnFamily;
    private Keyspace                        keyspace;
    private ConsistencyLevel                consistencyLevel    = DEFAULT_CONSISTENCY_LEVEL;
    private long                            lockTimeout         = DEFAULT_LOCK_TIMEOUT;
    private Long                            lockTtl             = DEFAULT_LOCK_TTL;
    private long                            pollInterval        = DEFAULT_POLL_WAIT;
    private SchedulerStats                  stats               = new CountingSchedulerStats();
    private String                          queueName           = DEFAULT_QUEUE_NAME;
    private AtomicLong                      counter             = new AtomicLong(new Random().nextInt(1000));
    private SchedulerHooks                  hooks               = new BaseSchedulerHook();
    private Function<String, Task>          invalidTaskHandler  = new Function<String, Task>() {
                                                                        @Override
                                                                        public Task apply(@Nullable String input) {
                                                                            return null;
                                                                        }
                                                                    };
    private List<SchedulerPartition>        partitions;
    private SchedulerSettings               settings            = new SchedulerSettings();
    
    private void log(String message) {
//        System.out.println(">>> " + Thread.currentThread().getName() + " " + message);
    }
    
    private void initialize() {
        Preconditions.checkArgument(
                lockTtl == null || lockTimeout < lockTtl, 
                "Timeout " + lockTtl + " must be less than TTL " + lockTtl);
        
        try {
            Column<SchedulerEntry> column = keyspace.prepareQuery(columnFamily)
                    .getRow(queueName)
                    .getColumn(SchedulerEntry.newMetadataEntry())
                    .execute()
                    .getResult();
            
            ByteArrayInputStream bais = new ByteArrayInputStream(column.getByteArrayValue());
            settings = mapper.readValue(bais, SchedulerSettings.class);
        } 
        catch (Exception e) {
        }
        
        partitions = Lists.newArrayList();
        
        for (int i = 0; i < settings.getPartitionCount(); i++) {
            for (int j = 0; j < settings.getShardCount(); j++) {
                partitions.add(new SchedulerPartition(queueName + ":" + i + ":" + j, i, j));
            }
        }
        
    }
    

    protected SchedulerEntry getBusyEntry(Task task) {
        return SchedulerEntry.newTaskEntry(task.getPriority(), task.getToken(), SchedulerEntryState.Busy);
    }
    
    /**
     * Return the shard for this task
     * @param task
     * @return
     */
    protected String getQueueKey(SchedulerEntry task) {
        return getQueueKey(TimeUUIDUtils.getMicrosTimeFromUUID(task.getTimestamp()));
    }
    
    /**
     * Return the shard for this timestamp
     * @param task
     * @return
     */
    private String getQueueKey(long taskTime) {
        long timePartition = (taskTime / settings.getPartitionDuration()) % settings.getPartitionCount();
        long shard         =  taskTime % settings.getShardCount();
        return queueName + ":" + timePartition + ":" + shard;
    }
    
    @Override
    public long getTaskCount() throws SchedulerException {
        Map<String, Integer> counts = getShardCounts();
        long count = 0;
        for (Integer value : counts.values()) {
            count += value;
        }
        return count;
    }
    
    @Override
    public Map<String, Integer> getShardCounts() throws SchedulerException {
        try {
            List<String> keys = Lists.newArrayList();
            for (int i = 0; i < settings.getPartitionCount(); i++) {
                for (int j = 0; j < settings.getShardCount(); j++) {
                    keys.add(queueName + ":" + i + ":" + j);
                }
            }
            
            Map<String, Integer> result = Maps.newTreeMap();
            result.putAll(keyspace.prepareQuery(columnFamily)
                    .getKeySlice(keys)
                    .getColumnCounts()
                    .execute()
                    .getResult());
            return result;
        } catch (ConnectionException e) {
            throw new SchedulerException("Failed to get counts", e);
        }
    }

    public String getCurrentPartition() {
        long timePartition = TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(),  TimeUnit.MILLISECONDS)/settings.getPartitionDuration();
        return queueName + ":" + ((timePartition + settings.getPartitionCount()) % settings.getPartitionCount());
    }

    @Override
    public void clearTasks() throws SchedulerException {
        // TODO:  Clear columns from all shards
    }

    @Override
    public void createScheduler() throws SchedulerException {
        try {
            keyspace.createColumnFamily(this.columnFamily, ImmutableMap.<String, Object>builder()
                            .put("key_validation_class",     "UTF8Type")
                            .put("comparator_type",          "CompositeType(BytesType, BytesType, TimeUUIDType, BytesType)")
                            .put("read_repair_chance",       1.0)
                            .put("gc_grace_period",          0)     // TODO: Calculate gc_grace_period
                            .build());
        } catch (ConnectionException e) {
            throw new SchedulerException("Failed to create column family for " + columnFamily.getName(), e);
        }
        
        try {
            // Convert the task object to JSON
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            mapper.writeValue(baos, settings);
            baos.flush();
            keyspace.prepareColumnMutation(columnFamily, queueName, SchedulerEntry.newMetadataEntry())
                    .putValue(baos.toByteArray(), null)
                    .execute();
        } catch (ConnectionException e) {
            throw new SchedulerException("Failed to create column family for " + columnFamily.getName(), e);
        } catch (Exception e) {
            throw new SchedulerException("Error serializing scheduler settings " + columnFamily.getName(), e);
        }
    }

    @Override
    public TaskConsumer createConsumer() {
        return new TaskConsumer() {
            private List<SchedulerPartition>    activePartitions    = Lists.newArrayList();
            private int                         index               = 0;

            {
                for (SchedulerPartition partition : partitions) {
                    activePartitions.add(partition);
                }
            }
            
            @Override
            public Collection<Task> acquireTasks(int itemsToPop) throws SchedulerException, BusyLockException, InterruptedException {
                while (true) {
                    if (index == activePartitions.size()) {
                        activePartitions.clear();
                        long currentPartition = TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)/settings.getPartitionDuration();
                        for (SchedulerPartition partition : partitions) {
                            if (partition.getPartition() == (currentPartition % settings.getPartitionCount())) {
                                activePartitions.add(partition);
                            }
                            else {
                                if (partition.getLastCount() != 0) {
                                    activePartitions.add(partition);
                                }
                            }
                        }
                        index = 0;
                        Collections.shuffle(activePartitions);
                    }
                    
                    try {
                        SchedulerPartition partition = activePartitions.get(index++);
                        Collection<Task> tasks = internalAcquireTasks(itemsToPop, partition.getName());
                        partition.setLastCount(tasks.size());
                        if (tasks.isEmpty()) {
                            stats.incEmptyPartitionCount();
                            Thread.sleep(pollInterval);
                            continue;
                        }
                        return tasks;
                    }
                    catch (BusyLockException e) {
                        Thread.sleep(pollInterval);
                    }
                }
            }
            
            private Collection<Task> internalAcquireTasks(int itemsToPop, String shardName) throws SchedulerException, BusyLockException {
                List<Task>  entries       = Lists.newArrayList();
                MutationBatch m           = null;
                SchedulerEntry lockColumn = null;
                ColumnListMutation<SchedulerEntry> rowMutation = null;
                
//                log("SHARD " + shardName);
                
                // Try locking first 
                try {
                    // 1. Write the lock column
                    lockColumn = SchedulerEntry.newLockEntry(SchedulerEntryState.None);
                    long curTimeMicros = TimeUUIDUtils.getTimeFromUUID(lockColumn.getTimestamp());
                    m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                    m.withRow(columnFamily, shardName)
                     .putColumn(lockColumn, curTimeMicros + lockTimeout, lockTtl.intValue());
                    m.execute();

                    // 2. Read back lock columns and entries
                    ColumnList<SchedulerEntry> result = keyspace.prepareQuery(columnFamily)
                            .setConsistencyLevel(consistencyLevel)
                            .getKey(shardName)
                            .withColumnRange(compositeSerializer.buildRange()
                                    .greaterThanEquals((short)SchedulerEntryType.Lock.ordinal())
                                    .lessThanEquals((short)SchedulerEntryType.Lock.ordinal())
                                    .build())
                            .execute()
                            .getResult();
                    
                    m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                    rowMutation = m.withRow(columnFamily, shardName);
                    rowMutation.deleteColumn(lockColumn);
                    
                    int lockCount = 0;
                    boolean lockAcquired = false;
                    for (Column<SchedulerEntry> column : result) {
                        SchedulerEntry lock = column.getName();
                        
                        log(shardName + " Phase1 : " + lock);
                        
                        // Stale lock so we can discard it
                        if (column.getLongValue() < curTimeMicros) {
                            log(shardName + " Discard lock");
                            stats.incExpiredLockCount();
                            rowMutation.deleteColumn(lock);
                        }
                        else if (lock.getState() == SchedulerEntryState.Acquired) {
                            log(shardName + " Already acquired");
                            throw new BusyLockException("Not first lock");
                        }
                        // This is our lock
                        else {
                            lockCount++;
                            if (lockCount == 1 && lock.getTimestamp().equals(lockColumn.getTimestamp())) {
                                lockAcquired = true;
                            }
                        }
                        
                        if (!lockAcquired) {
                            log(shardName + " Phase1 : Not our lock");
                            throw new BusyLockException("Not first lock");
                        }
                        
                        log(shardName + " Phase1 : Our lock");
                        
                        // Write the acquired lock column
                        lockColumn = SchedulerEntry.newLockEntry(lockColumn.getTimestamp(), SchedulerEntryState.Acquired);
                        rowMutation.putColumn(lockColumn, curTimeMicros + lockTimeout, lockTtl.intValue());
                        
                    }
                }
                catch (BusyLockException e) {
                    stats.incLockContentionCount();
                    throw e;
                }
                catch (ConnectionException e) {
                    throw new SchedulerException("Error", e);
                }
                finally {
                    try {
                        m.execute();
                    }
                    catch (Exception e) {
                        throw new SchedulerException("Error committing lock", e);
                    }
                }
                    
                long curTimeMicros = TimeUUIDUtils.getMicrosTimeFromUUID(lockColumn.getTimestamp());
                
                m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                rowMutation = m.withRow(columnFamily, shardName);
                rowMutation.deleteColumn(lockColumn);
                
                // 2. Read back lock columns and entries
                try {
                    ColumnList<SchedulerEntry> result = keyspace.prepareQuery(columnFamily)
                            .setConsistencyLevel(consistencyLevel)
                            .getKey(shardName)
                            .withColumnRange(new RangeBuilder()
                                .setLimit(itemsToPop)
                                .setEnd(compositeSerializer
                                        .makeEndpoint((short)SchedulerEntryType.Task.ordinal(), Equality.EQUAL)
                                        .append((short)0, Equality.EQUAL)
                                        .append(TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros), Equality.LESS_THAN_EQUALS).toBytes())
                                .build())
                        .execute()
                            .getResult();
                        
                    for (Column<SchedulerEntry> column : result) {
                        if (itemsToPop == 0) {
                            break;
                        }
                        
                        SchedulerEntry entry = column.getName();
                        log(shardName + "Phase2 " + entry);
                        
                        switch (entry.getType()) {
                            case Lock: 
                                // We have the lock
                                if (entry.getState() == SchedulerEntryState.Acquired) {
                                    log(shardName + " Phase2 : Not locked ");
                                    if (!entry.getTimestamp().equals(lockColumn.getTimestamp())) {
                                        throw new BusyLockException("Someone else snuck in");
                                    }
                                }
                                break;
                                
                            case Task: {
                                itemsToPop--; 
                                
                                // First, we always want to remove the old item
                                rowMutation.deleteColumn(entry);
                                
                                // Next, parse the Task metadata and add a timeout entry
                                Task task = null;
                                try {
                                    ByteArrayInputStream bais = new ByteArrayInputStream(column.getByteArrayValue());
                                    task = mapper.readValue(bais, Task.class);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    // Error parsing the task so we pass it on to the invalid task handler.
                                    try {
                                        task = invalidTaskHandler.apply(column.getStringValue());
                                    }
                                    catch (Exception e2) {
                                        // TODO:
                                        e2.printStackTrace();
                                    }
                                } 
                                
                                // Update the task state
                                if (task != null) {
                                    entries.add(task);
                                    
                                    if (task.getTimeout() != 0) {
                                        SchedulerEntry timeoutEntry = SchedulerEntry.newTaskEntry(
                                                entry.getPriority(),
                                                TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros + TimeUnit.MICROSECONDS.convert(task.getTimeout(), TimeUnit.SECONDS)), 
                                                SchedulerEntryState.Busy);
                                        
                                        task.setToken(timeoutEntry.getTimestamp());
                                        
                                        m.withRow(columnFamily, getQueueKey(timeoutEntry))
                                         .putColumn(timeoutEntry, column.getStringValue());
                                    }
                                    else {
                                        task.setToken(null);
                                    }
                                    
                                    // Update some stats
                                    switch (entry.getState()) {
                                    case Waiting:
                                        stats.incProcessCount();
                                        break;
                                    case Busy:
                                        stats.incReprocessCount();
                                        break;
                                    default:
                                        // TODO:
                                        break;
                                    }
                                }
                                // The task metadata was invalid so we just get rid of it.
                                else {
                                    stats.incInvalidTaskCount();
                                }
                                break;
                            }
                            default: {
                                log("****");
                                // TODO: Error: Unknown type
                                break;
                            }
                        }
                    }
                    
//                            log(shardName + " count: " + entries.size() + " of " + result.size());
                    hooks.preAcquireTasks(entries, m);
                    return entries;
                }
                catch (BusyLockException e) {
                    stats.incLockContentionCount();
                    throw e;
                }
                catch (Exception e) {
                    e.printStackTrace();
                    throw new SchedulerException("Error processing scheduler queue : " + shardName, e);
                }
                // 3. Release lock and remove any acquired entries
                finally {
                    try {
                        m.execute();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        // Hmmm...
                    }
                }
            }
            
            @Override
            public void ackTask(Task task) throws SchedulerException {
                SchedulerEntry entry = getBusyEntry(task);
                stats.incFinishTaskCount();
                
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                mb.withRow(columnFamily, getQueueKey(entry))
                    .deleteColumn(entry);
                
                hooks.preAckTask(task, mb);
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new SchedulerException("Failed to ack task", e);
                }
            }

            @Override
            public void ackTasks(Collection<Task> tasks) throws SchedulerException {
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                for (Task task : tasks) {
                    if (task.getToken() != null) {
                        SchedulerEntry entry = getBusyEntry(task);
                        stats.incFinishTaskCount();
                        mb.withRow(columnFamily, getQueueKey(entry))
                          .deleteColumn(entry);
                    }
                }
                
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new SchedulerException("Failed to ack tasks", e);
                }
            }            
        };
    }

    @Override
    public TaskProducer createProducer() {
        return new TaskProducer() {

            @Override
            public UUID scheduleTask(Task task) throws SchedulerException {
                // Get the execution time from the task or set to current time so it runs immediately
                long curTimeMicros;
                if (task.getNextTriggerTime() == 0) {
                    curTimeMicros = TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                  + (counter.incrementAndGet() % 1000);
                }
                else {
                    curTimeMicros = TimeUnit.MICROSECONDS.convert(task.getNextTriggerTime(),  TimeUnit.SECONDS)
                                  + (counter.incrementAndGet() % 1000000);
                }

                // Update the task for the new token
                task.setToken(TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros));
                
                // Set up the queue entry
                SchedulerEntry entry = SchedulerEntry.newTaskEntry(
                        task.getPriority(), 
                        task.getToken(), 
                        SchedulerEntryState.Waiting);

                // Convert the task object to JSON
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try {
                    mapper.writeValue(baos, task);
                    baos.flush();
                } catch (Exception e) {
                    throw new SchedulerException("Failed to serialize task data", e);
                }

                log(getQueueKey(entry) + " WRITE");
                
                // Write the mutation 
                MutationBatch mb = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                mb.withRow(columnFamily, getQueueKey(entry))
                  .putColumn(entry, new String(baos.toByteArray()), (int)settings.getVisibilityTimeout());
                    
                hooks.preScheduleTask(task, mb);
                try {
                    mb.execute();
                } catch (ConnectionException e) {
                    throw new SchedulerException("Failed to insert task into queue", e);
                }
                
                // Update state and retun the token
                stats.incSubmitTaskCount();
                return task.getToken();
            }
        };
    }

}

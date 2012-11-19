package com.netflix.astyanax.recipes.scheduler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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
import com.netflix.astyanax.retry.ExponentialBackoff;
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
 *      Column: <type><timeuuid><state><instance>
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
 *  
 * 
 * @author elandau
 *
 */
public class ShardedDistributedScheduler implements TaskScheduler {
    
    public static final String           DEFAULT_COLUMN_FAMILY_NAME      = "Scheduler";
    public static final String           DEFAULT_QUEUE_NAME              = "Queue";
    public static final long             DEFAULT_VISIBILITY_TIMEOUT      = TimeUnit.MICROSECONDS.convert(4,  TimeUnit.DAYS);
    public static final long             DEFAULT_LOCK_TIMEOUT            = TimeUnit.MICROSECONDS.convert(1,  TimeUnit.MINUTES);
    public static final long             DEFAULT_LOCK_TTL                = TimeUnit.MICROSECONDS.convert(10, TimeUnit.MINUTES);
    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL       = ConsistencyLevel.CL_LOCAL_QUORUM;
    public static final RetryPolicy      DEFAULT_RETRY_POLICY            = RunOnce.get();
    public static final long             DEFAULT_POLL_INTERVAL           = TimeUnit.MILLISECONDS.convert(100, TimeUnit.MILLISECONDS);
    public static final int              DEFAULT_SHARD_COUNT             = 10;
    public static final int              DEFAULT_BUCKET_DURATION         = (int)TimeUnit.MILLISECONDS.convert(1,  TimeUnit.HOURS);
    public static final int              DEFAULT_BUCKET_COUNT            = 10;
    
    public static class Builder {
        private ShardedDistributedScheduler scheduler        = new ShardedDistributedScheduler();
        private String                      columnFamilyName = DEFAULT_COLUMN_FAMILY_NAME;

        public Builder withColumnFamily(String columnFamilyName) {
            this.columnFamilyName = columnFamilyName;
            return this;
        }
        
        public Builder withShardCount(int count) {
            scheduler.shardCount = count;
            return this;
        }
        
        public Builder withBuckets(int bucketCount, int bucketDuration, TimeUnit units) {
            scheduler.partitionDuration = (int)TimeUnit.MILLISECONDS.convert(bucketDuration,  units);
            scheduler.partitionCount    = bucketCount;
            return this;
        }
        
        public Builder withVisibilityTimeout(Long timeout, TimeUnit units) {
            scheduler.visibilityTimeout = TimeUnit.MICROSECONDS.convert(timeout,  units);
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
        
        public Builder withInstanceId(String id) {
            scheduler.instanceId = id;
            return this;
        }
        
        public Builder withLockRetryPolicy(RetryPolicy policy) {
            scheduler.lockRetryPolicy = policy;
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
            Preconditions.checkArgument(
                    scheduler.lockTtl == null || scheduler.lockTimeout < scheduler.lockTtl, 
                    "Timeout " + scheduler.lockTtl + " must be less than TTL " + scheduler.lockTtl);
            
            scheduler.columnFamily = ColumnFamily.newColumnFamily(columnFamilyName, StringSerializer.get(), compositeSerializer); 
            scheduler.lockColumn = new SchedulerEntry(
                    SchedulerEntryType.Lock, 
                    TimeUUIDUtils.getUniqueTimeUUIDinMicros(), 
                    SchedulerEntryState.None, 
                    scheduler.instanceId);
            
            return scheduler;
        }
    }

    private final static AnnotatedCompositeSerializer<SchedulerEntry> compositeSerializer = new AnnotatedCompositeSerializer<SchedulerEntry>(SchedulerEntry.class);
    
    // Immutable after configuration
    private ColumnFamily<String, SchedulerEntry> columnFamily;
    private Keyspace                        keyspace;
    private String                          queueName           = DEFAULT_QUEUE_NAME;
    private ConsistencyLevel                consistencyLevel    = DEFAULT_CONSISTENCY_LEVEL;;
    private long                            visibilityTimeout   = DEFAULT_VISIBILITY_TIMEOUT;
    private long                            lockTimeout         = DEFAULT_LOCK_TIMEOUT;
    private Long                            lockTtl             = DEFAULT_LOCK_TTL;
    private long                            pollInterval        = DEFAULT_POLL_INTERVAL;
    private RetryPolicy                     lockRetryPolicy     = new ExponentialBackoff(50, 10);
    private String                          instanceId;
    private AtomicLong                      contentionCounter   = new AtomicLong();
    private int                             shardCount          = DEFAULT_SHARD_COUNT;
    private int                             partitionDuration   = DEFAULT_BUCKET_DURATION;
    private int                             partitionCount      = DEFAULT_BUCKET_COUNT;
    private SchedulerStats                  stats               = new CountingSchedulerStats();
    private List<String>                    partitions          = Lists.newArrayList();
    private SchedulerEntry                  lockColumn;
    private SchedulerHooks                  hooks               = new BaseSchedulerHook();
    
    @Override
    public void scheduleTask(Task task) throws SchedulerException {
        long curTimeMicros = task.getNextTriggerTime();
        if (curTimeMicros == 0)
            curTimeMicros = getCurrentTimeMicros();

        SchedulerEntry entry = new SchedulerEntry(SchedulerEntryType.Element, TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros), SchedulerEntryState.Waiting, null);
        
        stats.incSubmitTaskCount();
        
        MutationBatch mb = keyspace.prepareMutationBatch();
        mb.withRow(columnFamily, getQueueKey(entry))
            .putColumn(entry, task.getData());
            
        hooks.preScheduleTask(task, mb);
        try {
            mb.execute();
        } catch (ConnectionException e) {
            throw new SchedulerException("Failed to insert task into queue", e);
        }
    }
    
    @Override
    public Collection<Task> acquireTasks(int itemsToPop) throws SchedulerException, BusyLockException, InterruptedException {
        while (true) {
            if (partitions.isEmpty()) {
                long timePartition = System.currentTimeMillis() / partitionDuration;
                for (int i = 0; i < 2; i++) {
                    for (int j = 0; j < shardCount; j++) {
                        partitions.add(queueName + ":" + ((timePartition - i) % partitionCount) + ":" + j);
                    }
                }
                Collections.shuffle(partitions);
            }
            
            Collection<Task> tasks = internalAcquireTasks(itemsToPop, partitions.remove(0));
            if (tasks.isEmpty()) {
                stats.incEmptyPartitionCount();
                Thread.sleep(this.pollInterval);
                continue;
            }
            return tasks;
        }
    }
    
    private Collection<Task> internalAcquireTasks(int itemsToPop, String shardName) throws SchedulerException, BusyLockException {
        RetryPolicy retry = lockRetryPolicy.duplicate();
        List<Task> entries = Lists.newArrayList();
        while (true) {
            long curTimeMicros = getCurrentTimeMicros();
            
            // 1. Write the lock column
            try {
                MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                m.withRow(columnFamily, shardName)
                 .putColumn(lockColumn, TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS) + lockTimeout, lockTtl.intValue());
                m.execute();
            }
            catch (Exception e) {
                continue;
            }
                
            // 2. Read back lock columns and entries
            MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
            ColumnListMutation<SchedulerEntry> rowMutation = m.withRow(columnFamily, shardName);
            try {
                ColumnList<SchedulerEntry> result = keyspace.prepareQuery(columnFamily)
                        .setConsistencyLevel(consistencyLevel)
                        .getKey(shardName)
                        .withColumnRange(new RangeBuilder()
                            .setLimit(2 + itemsToPop)   // +2 to include the lock column and maybe one stale lock
                            .setEnd(compositeSerializer
                                    .makeEndpoint((short)SchedulerEntryType.Element.ordinal(), Equality.EQUAL)
                                    .append(TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros), Equality.LESS_THAN_EQUALS).toBytes())
                            .build())
                    .execute()
                        .getResult();
                
                boolean lockAcquired = false;
                int lockCount = 0;
                for (Column<SchedulerEntry> column : result) {
                    SchedulerEntry entry = column.getName();
                    switch (entry.getType()) {
                        case Lock: 
                            // We have the lock
                            if (entry.getTimestamp().equals(lockColumn.getTimestamp())) {
                                lockAcquired = true;
                                lockCount++;
                                rowMutation.deleteColumn(entry);
                            }
                            // Not our lock
                            else {
                                // Is this a stale lock
                                if (column.getLongValue() < curTimeMicros) {
                                    stats.incExpiredLockCount();
                                    rowMutation.deleteColumn(entry);
                                }
                                else {
                                    lockCount++;
                                }
                            }
                            break;
                        case Element: {
                            if (lockAcquired && lockCount == 1) {
                                if (itemsToPop-- < 0) 
                                    break;
                                switch (entry.getState()) {
                                case Waiting:
                                    stats.incProcessCount();
                                    break;
                                case Busy:
                                    stats.incReprocessCount();
                                    break;
                                default:
                                    // TODOD
                                    break;
                                }
                                
                                SchedulerEntry timeoutEntry = new SchedulerEntry(
                                        SchedulerEntryType.Element, 
                                        TimeUUIDUtils.getMicrosTimeUUID(curTimeMicros + this.visibilityTimeout), 
                                        SchedulerEntryState.Busy, 
                                        instanceId);
                                
                                entries.add(new Task(timeoutEntry.getTimestamp(), column.getStringValue()));
                                rowMutation.deleteColumn(entry);
                                m.withRow(columnFamily, getQueueKey(timeoutEntry)).putColumn(timeoutEntry, column.getStringValue());
                            }
                            else {
                                stats.incLockContentionCount();
                                throw new BusyLockException("Row " + shardName + " locked by " + entry.getInstance());
                            }
                            break;
                        }
                        default: {
                            // TODO: Error: Unknown type
                            break;
                        }
                    }
                }
                
                hooks.preAcquireTasks(entries, m);
                return entries;
            }
            catch (BusyLockException e) {
                contentionCounter.incrementAndGet();
                try {
                    m.execute();
                }
                catch (Exception e2) {
                    // Hmmm...
                }
                if (!retry.allowRetry()) {
                    throw e;
                }
            } catch (ConnectionException e) {
                throw new SchedulerException("Error querying queue : " + shardName, e);
            }
            // 3. Release lock and move the entries
            finally {
                try {
                    m.execute();
                }
                catch (Exception e) {
                    // Hmmm...
                }
            }
        }
    }
    
    @Override
    public void ackTask(Task task) throws SchedulerException {
        SchedulerEntry entry = new SchedulerEntry(SchedulerEntryType.Element, task.getTaskId(), SchedulerEntryState.Busy, instanceId);
        stats.incFinishTaskCount();
        
        MutationBatch mb = keyspace.prepareMutationBatch();
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
        MutationBatch mb = keyspace.prepareMutationBatch();
        for (Task task : tasks) {
            SchedulerEntry entry = new SchedulerEntry(SchedulerEntryType.Element, task.getTaskId(), SchedulerEntryState.Busy, instanceId);
            stats.incFinishTaskCount();
            mb.withRow(columnFamily, getQueueKey(entry))
                .deleteColumn(entry);
        }
        
        try {
            mb.execute();
        } catch (ConnectionException e) {
            throw new SchedulerException("Failed to ack tasks", e);
        }
    }

    /**
     * Return the shard for this task
     * @param task
     * @return
     */
    private String getQueueKey(SchedulerEntry task) {
        return getQueueKey(TimeUUIDUtils.getMicrosTimeFromUUID(task.getTimestamp())/1000);
    }
    
    /**
     * Return the shard for this timestamp
     * @param task
     * @return
     */
    private String getQueueKey(long taskTime) {
        long timePartition = (taskTime / partitionDuration) % partitionCount;
        long shard         =  taskTime % shardCount;
        return queueName + ":" + timePartition + ":" + shard;
    }
    
    /**
     * Get the current system time
     * 
     * @return
     */
    private static long getCurrentTimeMicros() {
        return TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public long getTaskCount() {
        return 0;
    }

    @Override
    public void clearTasks() throws SchedulerException {
        // TODO Auto-generated method stub
        
    }

}

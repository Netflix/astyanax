package com.netflix.astyanax.recipes.locks;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnMap;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.OrderedColumnMap;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnce;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.LongSerializer;

public class OneStepDistributedRowLock<K, C> implements DistributedRowLock {
    public static final int      LOCK_TIMEOUT                    = 60;
    public static final TimeUnit DEFAULT_OPERATION_TIMEOUT_UNITS = TimeUnit.MINUTES;

    private final ColumnFamily<K, C> columnFamily;      // The column family for data and lock
    private final Keyspace   keyspace;                  // The keyspace
    private final K          key;                       // Key being locked

    private long             timeout          = LOCK_TIMEOUT;                   // Timeout after which the lock expires.  Units defined by timeoutUnits.
    private TimeUnit         timeoutUnits     = DEFAULT_OPERATION_TIMEOUT_UNITS;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.CL_LOCAL_QUORUM;
    private boolean          failOnStaleLock  = false;           
    private Set<C>           locksToDelete    = Sets.newHashSet();
    private C                lockColumn       = null;
    private ColumnMap<C>     columns          = null;
    private Integer          ttl              = null;                           // Units in seconds
    private boolean          readDataColumns  = false;
    private RetryPolicy      backoffPolicy    = RunOnce.get();
    private long             acquireTime      = 0;
    private int              retryCount       = 0;
    private LockColumnStrategy<C> columnStrategy = null;
    
    public OneStepDistributedRowLock(Keyspace keyspace, ColumnFamily<K, C> columnFamily, K key) {
        this.keyspace     = keyspace;
        this.columnFamily = columnFamily;
        this.key          = key;
    }

    public OneStepDistributedRowLock<K, C> withColumnStrategy(LockColumnStrategy<C> columnStrategy) {
        this.columnStrategy = columnStrategy;
        return this;
    }
    
    /**
     * Modify the consistency level being used. Consistency should always be a
     * variant of quorum. The default is CL_QUORUM, which is OK for single
     * region. For multi region the consistency level should be CL_LOCAL_QUORUM.
     * CL_EACH_QUORUM can be used but will Incur substantial latency.
     * 
     * @param consistencyLevel
     * @return
     */
    public OneStepDistributedRowLock<K, C> withConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    /**
     * If true the first read will also fetch all the columns in the row as 
     * opposed to just the lock columns.
     * @param flag
     * @return
     */
    public OneStepDistributedRowLock<K, C> withDataColumns(boolean flag) {
        this.readDataColumns = flag;
        return this;
    }
    
    /**
     * When set to true the operation will fail if a stale lock is detected
     * 
     * @param failOnStaleLock
     * @return
     */
    public OneStepDistributedRowLock<K, C> failOnStaleLock(boolean failOnStaleLock) {
        this.failOnStaleLock = failOnStaleLock;
        return this;
    }

    /**
     * Time for failed locks. Under normal circumstances the lock column will be
     * deleted. If not then this lock column will remain and the row will remain
     * locked. The lock will expire after this timeout.
     * 
     * @param timeout
     * @param unit
     * @return
     */
    public OneStepDistributedRowLock<K, C> expireLockAfter(long timeout, TimeUnit unit) {
        this.timeout      = timeout;
        this.timeoutUnits = unit;
        return this;
    }

    /**
     * This is the TTL on the lock column being written, as opposed to expireLockAfter which 
     * is written as the lock column value.  Whereas the expireLockAfter can be used to 
     * identify a stale or abandoned lock the TTL will result in the stale or abandoned lock
     * being eventually deleted by cassandra.  Set the TTL to a number that is much greater
     * tan the expireLockAfter time.
     * @param ttl
     * @return
     */
    public OneStepDistributedRowLock<K, C> withTtl(Integer ttl) {
        this.ttl = ttl;
        return this;
    }
    
    public OneStepDistributedRowLock<K, C> withTtl(Integer ttl, TimeUnit units) {
        this.ttl = (int) TimeUnit.SECONDS.convert(ttl,  units);
        return this;
    }
    
    public OneStepDistributedRowLock<K, C> withBackoff(RetryPolicy policy) {
        this.backoffPolicy  = policy;
        return this;
    }

    /**
     * Try to take the lock.  The caller must call .release() to properly clean up
     * the lock columns from cassandra
     * 
     * @return
     * @throws Exception
     */
    @Override
    public void acquire() throws Exception {
        
        Preconditions.checkArgument(ttl == null || TimeUnit.SECONDS.convert(timeout, timeoutUnits) < ttl, "Timeout " + timeout + " must be less than TTL " + ttl);
        
        RetryPolicy retry = backoffPolicy.duplicate();
        retryCount = 0;
        while (true) {
            try {
                long curTimeMicros = getCurrentTimeMicros();
                
                MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
                fillLockMutation(m, curTimeMicros, ttl);
                m.execute();
                
                verifyLock(curTimeMicros);
                acquireTime = System.currentTimeMillis();
                return;
            }
            catch (BusyLockException e) {
                release();
                if(!retry.allowRetry())
                    throw e;
                retryCount++;
            }
        }
    }

    /**
     * Take the lock and return the row data columns.  Use this, instead of acquire, when you 
     * want to implement a read-modify-write scenario and want to reduce the number of calls
     * to Cassandra.
     * 
     * @return
     * @throws Exception
     */
    public ColumnMap<C> acquireLockAndReadRow() throws Exception {
        withDataColumns(true);
        acquire();
        return getDataColumns();
    }
    
    /**
     * Verify that the lock was acquired.  This shouldn't be called unless it's part of a recipe
     * built on top of AbstractDistributedRowLock.  
     * 
     * @param curTimeInMicros
     * @throws BusyLockException
     */
    public void verifyLock(long curTimeInMicros) throws Exception, BusyLockException, StaleLockException {
        if (getLockColumn() == null) 
            throw new IllegalStateException("verifyLock() called without attempting to take the lock");
        
        // Read back all columns. There should be only 1 if we got the lock
        Map<C, Long> lockResult = readLockColumns(readDataColumns);

        // Cleanup and check that we really got the lock
        for (Entry<C, Long> entry : lockResult.entrySet()) {
            // This is a stale lock that was never cleaned up
            if (entry.getValue() != 0 && curTimeInMicros > entry.getValue()) {
                if (failOnStaleLock) {
                    throw new StaleLockException("Stale lock on row '" + key + "'.  Manual cleanup requried.");
                }
                locksToDelete.add(entry.getKey());
            }
            // Lock already taken, and not by us
            else if (!entry.getKey().equals(getLockColumn())) {
                throw new BusyLockException("Lock already acquired for row '" + key + "' with lock column " + entry.getKey());
            }
        }
    }

    /**
     * Release the lock by releasing this and any other stale lock columns
     */
    @Override
    public void release() throws Exception {
        if (!locksToDelete.isEmpty() || getLockColumn() != null) {
            MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
            fillReleaseMutation(m, false);
            m.execute();
        }
    }

    /**
     * Release using the provided mutation.  Use this when you want to commit actual data
     * when releasing the lock
     * @param m
     * @throws Exception
     */
    public void releaseWithMutation(MutationBatch m) throws Exception {
        releaseWithMutation(m, false);
    }
    
    public boolean releaseWithMutation(MutationBatch m, boolean force) throws Exception {
        long elapsed = System.currentTimeMillis() - acquireTime;
        boolean isStale = false;
        if (timeout > 0 && elapsed > TimeUnit.MILLISECONDS.convert(timeout, this.timeoutUnits)) {
            isStale = true;
            if (!force) {
                throw new StaleLockException("Lock for '" + getKey() + "' became stale");
            }
        }
        
        m.setConsistencyLevel(consistencyLevel);
        fillReleaseMutation(m, false);
        m.execute();
        
        return isStale;
    }
    
    /**
     * Return a mapping of existing lock columns and their expiration times
     * 
     * @return
     * @throws Exception
     */
    public Map<C, Long> readLockColumns() throws Exception {
        return readLockColumns(false);
    }
    
    /**
     * Read all the lock columns.  Will also ready data columns if withDataColumns(true) was called
     * 
     * @param readDataColumns
     * @return
     * @throws Exception
     */
    private Map<C, Long> readLockColumns(boolean readDataColumns) throws Exception {
        Map<C, Long> result = Maps.newLinkedHashMap();
        // Read all the columns
        if (readDataColumns) {
            columns = new OrderedColumnMap<C>();
            ColumnList<C> lockResult = keyspace
                .prepareQuery(columnFamily)
                    .setConsistencyLevel(consistencyLevel)
                    .getKey(key)
                .execute()
                    .getResult();
    
            for (Column<C> c : lockResult) {
                if (columnStrategy.isLockColumn(c.getName()))
                    result.put(c.getName(), readTimeoutValue(c));
                else 
                    columns.add(c);
            }
        }
        // Read only the lock columns
        else {
            ColumnList<C> lockResult = keyspace
                .prepareQuery(columnFamily)
                    .setConsistencyLevel(consistencyLevel)
                    .getKey(key)
                    .withColumnRange(columnStrategy.getLockColumnRange())
                .execute()
                    .getResult();

            for (Column<C> c : lockResult) {
                result.put(c.getName(), readTimeoutValue(c));
            }

        }
        return result;    
    }
    
    /**
     * Release all locks. Use this carefully as it could release a lock for a
     * running operation.
     * 
     * @return
     * @throws Exception
     */
    public Map<C, Long> releaseAllLocks() throws Exception {
        return releaseLocks(true);
    }

    /**
     * Release all expired locks for this key.
     * 
     * @return
     * @throws Exception
     */
    public Map<C, Long> releaseExpiredLocks() throws Exception {
        return releaseLocks(false);
    }

    /**
     * Delete locks columns. Set force=true to remove locks that haven't 
     * expired yet.
     * 
     * This operation first issues a read to cassandra and then deletes columns
     * in the response.
     * 
     * @param force - Force delete of non expired locks as well
     * @return
     * @throws Exception
     */
    public Map<C, Long> releaseLocks(boolean force) throws Exception {
        Map<C, Long> locksToDelete = readLockColumns();

        MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
        ColumnListMutation<C> row = m.withRow(columnFamily, key);
        long now = getCurrentTimeMicros();
        for (Entry<C, Long> c : locksToDelete.entrySet()) {
            if (force || (c.getValue() > 0 && c.getValue() < now)) {
                row.deleteColumn(c.getKey());
            }
        }
        m.execute();

        return locksToDelete;
    }

    /**
     * Get the current system time
     * 
     * @return
     */
    private static long getCurrentTimeMicros() {
        return TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Fill a mutation with the lock column. This may be used when the mutation
     * is executed externally but should be used with extreme caution to ensure
     * the lock is properly released
     * 
     * @param m
     * @param time
     * @param ttl
     */
    public C fillLockMutation(MutationBatch m, Long time, Integer ttl) {
        if (lockColumn != null) {
            if (!lockColumn.equals(columnStrategy.generateLockColumn()))
                throw new IllegalStateException("Can't change prefix or lockId after acquiring the lock");
        }
        else {
            lockColumn = columnStrategy.generateLockColumn();
        }
        
        Long timeoutValue 
              = (time == null)
              ? new Long(0)
              : time + TimeUnit.MICROSECONDS.convert(timeout, timeoutUnits);
              
        m.withRow(columnFamily, key).putColumn(lockColumn, generateTimeoutValue(timeoutValue), ttl);
        return lockColumn;
    }
    
    /**
     * Generate the expire time value to put in the column value.
     * @param timeout
     * @return
     */
    private ByteBuffer generateTimeoutValue(long timeout) {
        if (columnFamily.getDefaultValueSerializer() == ByteBufferSerializer.get() ||
            columnFamily.getDefaultValueSerializer() == LongSerializer.get()) {
            return LongSerializer.get().toByteBuffer(timeout);
        }
        else {
            return columnFamily.getDefaultValueSerializer().fromString(Long.toString(timeout));
        }
    }
    
    /**
     * Read the expiration time from the column value
     * @param column
     * @return
     */
    public long readTimeoutValue(Column<?> column) {
        if (columnFamily.getDefaultValueSerializer() == ByteBufferSerializer.get() ||
            columnFamily.getDefaultValueSerializer() == LongSerializer.get()) {
            return column.getLongValue();
        }
        else {
            return Long.parseLong(column.getStringValue());
        }
    }

    /**
     * Fill a mutation that will release the locks. This may be used from a
     * separate recipe to release multiple locks.
     * 
     * @param m
     */
    public void fillReleaseMutation(MutationBatch m, boolean excludeCurrentLock) {
        // Add the deletes to the end of the mutation
        ColumnListMutation<C> row = m.withRow(columnFamily, key);
        for (C c : locksToDelete) {
            row.deleteColumn(c);
        }
        if (!excludeCurrentLock && lockColumn != null) 
            row.deleteColumn(lockColumn);
        locksToDelete.clear();
        lockColumn = null;
    }


    public ColumnMap<C> getDataColumns() {
        return columns;
    }
    
    public K getKey() {
        return key;
    }
    
    public Keyspace getKeyspace() {
        return keyspace;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public C getLockColumn() {
        return lockColumn;
    }
    
    public int getRetryCount() {
        return retryCount;
    }
}
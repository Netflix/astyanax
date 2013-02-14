/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.recipes.locks;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.TimeUUIDUtils;

/**
 * Takes a distributed row lock for a single row.  The row lock is accomplished using
 * a sequence of read/write events to Cassandra without the need for something like
 * zookeeper.  
 * 
 * Algorithm 
 * 1. Write a column with name <prefix>_<uuid>. Value is an expiration time. 
 * 2. Read back all columns with <prefix> 
 *      case 1) count==1 Got the lock 
 *      case 2) count> 1 No lock
 * 3. Do something in your code assuming the row is locked
 * 4. Release the lock by deleting the lock columns
 * 
 * Usage considerations
 * 1. Set an expiration time (expireLockAfter) that is long enough for your processing to complete
 * 2. Use this when the probability for contension is very low
 * 3. Optimize by reading all columns (withIncludeAllColumn(true)) and merge the mutation
 *      into the release.  This will save 2 calls to cassandra.
 * 4. If the client fails after Step 1.  A subsequent attempt to lock will automatically 
 *      release these stale locks.  You can turn this auto cleanup off by calling
 *      failOnStaleLock(false), handling a StaleLockException and doing manual cleanup by
 *      calling releaseExpiredLocks()
 * 5. An optional TTL can be set on the lock columns which will ensure abandoned/stale locks
 *      will be cleaned up by compactions at some point.
 * 6. You can customize the 'prefix' used for the lock columns.  This will help with storing
 *      the lock columns with data in the same row.  
 * 7. You can customize the unique part of the lock column to include meaningful data such
 *      as the UUID row key from another column family.  This can have the same effect as 
 *      assigning a foreign key to the lock column and is useful for uniqueness constraint.
 * 8. This recipe is not a transaction.  
 * 
 * Take a lock,
 * <code>
 *      ColumnPrefixDistributedRowLock<String> lock = new ColumnPrefixDistributedRowLock<String>(keyspace, columnFamily, "KeyBeingLocked");
 *      try {
 *          lock.acquire();
 *      }
 *      finally {
 *          lock.release();
 *      }
 * </code>
 * 
 * Read, Modify, Write.  The read, modify, write piggybacks on top of the lock calls.
 * 
 * <code>
 *      ColumnPrefixDistributedRowLock<String> lock = new ColumnPrefixDistributedRowLock<String>(keyspace, columnFamily, "KeyBeingLocked");
 *      MutationBatch m = keyspace.prepareMutationBatch();
 *      try {
 *          ColumnMap<String> columns = lock.acquireLockAndReadRow();
 *          
 *          m.withRow("KeyBeingLocked")
 *              .putColumn("SomeColumnBeingUpdated", );
 *              
 *          lock.releaseWithMutation(m);
 *      }
 *      catch (Exception e) {
 *          lock.release();
 *      }
 * </code>
 * 
 * @author elandau
 * 
 * @param <K>
 */
public class ColumnPrefixDistributedRowLock<K> implements DistributedRowLock {
    public static final int      LOCK_TIMEOUT                    = 60;
    public static final TimeUnit DEFAULT_OPERATION_TIMEOUT_UNITS = TimeUnit.MINUTES;
    public static final String   DEFAULT_LOCK_PREFIX             = "_LOCK_";

    private final ColumnFamily<K, String> columnFamily; // The column family for data and lock
    private final Keyspace   keyspace;                  // The keyspace
    private final K          key;                       // Key being locked

    private long             timeout          = LOCK_TIMEOUT;                   // Timeout after which the lock expires.  Units defined by timeoutUnits.
    private TimeUnit         timeoutUnits     = DEFAULT_OPERATION_TIMEOUT_UNITS;
    private String           prefix           = DEFAULT_LOCK_PREFIX;            // Prefix to identify the lock columns
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.CL_LOCAL_QUORUM;
    private boolean          failOnStaleLock  = false;           
    private String           lockColumn       = null;
    private String           lockId           = null;
    private Set<String>      locksToDelete    = Sets.newHashSet();
    private ColumnMap<String> columns         = null;
    private Integer          ttl              = null;                           // Units in seconds
    private boolean          readDataColumns  = false;
    private RetryPolicy      backoffPolicy    = RunOnce.get();
    private long             acquireTime      = 0;
    private int              retryCount       = 0;

    public ColumnPrefixDistributedRowLock(Keyspace keyspace, ColumnFamily<K, String> columnFamily, K key) {
        this.keyspace     = keyspace;
        this.columnFamily = columnFamily;
        this.key          = key;
        this.lockId       = TimeUUIDUtils.getUniqueTimeUUIDinMicros().toString();
    }

    /**
     * Modify the consistency level being used. Consistency should always be a
     * variant of quorum. The default is CL_QUORUM, which is OK for single
     * region. For multi region the consistency level should be CL_LOCAL_QUORUM.
     * CL_EACH_QUORUM can be used but will Incur substantial latency.
     * 
     * @param consistencyLevel
     */
    public ColumnPrefixDistributedRowLock<K> withConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    /**
     * Specify the prefix that uniquely distinguishes the lock columns from data
     * column
     * 
     * @param prefix
     */
    public ColumnPrefixDistributedRowLock<K> withColumnPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    /**
     * If true the first read will also fetch all the columns in the row as 
     * opposed to just the lock columns.
     * @param flag
     */
    public ColumnPrefixDistributedRowLock<K> withDataColumns(boolean flag) {
        this.readDataColumns = flag;
        return this;
    }
    
    /**
     * Override the autogenerated lock column.
     * 
     * @param lockId
     */
    public ColumnPrefixDistributedRowLock<K> withLockId(String lockId) {
        this.lockId = lockId;
        return this;
    }

    /**
     * When set to true the operation will fail if a stale lock is detected
     * 
     * @param failOnStaleLock
     */
    public ColumnPrefixDistributedRowLock<K> failOnStaleLock(boolean failOnStaleLock) {
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
     */
    public ColumnPrefixDistributedRowLock<K> expireLockAfter(long timeout, TimeUnit unit) {
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
     */
    public ColumnPrefixDistributedRowLock<K> withTtl(Integer ttl) {
        this.ttl = ttl;
        return this;
    }
    
    public ColumnPrefixDistributedRowLock<K> withTtl(Integer ttl, TimeUnit units) {
        this.ttl = (int) TimeUnit.SECONDS.convert(ttl,  units);
        return this;
    }
    
    public ColumnPrefixDistributedRowLock<K> withBackoff(RetryPolicy policy) {
        this.backoffPolicy  = policy;
        return this;
    }

    /**
     * Try to take the lock.  The caller must call .release() to properly clean up
     * the lock columns from cassandra
     * 
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
     * @throws Exception
     */
    public ColumnMap<String> acquireLockAndReadRow() throws Exception {
        withDataColumns(true);
        acquire();
        return getDataColumns();
    }
    
    /**
     * Verify that the lock was acquired.  This shouldn't be called unless it's part of a recipe
     * built on top of ColumnPrefixDistributedRowLock.  
     * 
     * @param curTimeInMicros
     * @throws BusyLockException
     */
    public void verifyLock(long curTimeInMicros) throws Exception, BusyLockException, StaleLockException {
        if (lockColumn == null) 
            throw new IllegalStateException("verifyLock() called without attempting to take the lock");
        
        // Read back all columns. There should be only 1 if we got the lock
        Map<String, Long> lockResult = readLockColumns(readDataColumns);

        // Cleanup and check that we really got the lock
        for (Entry<String, Long> entry : lockResult.entrySet()) {
            // This is a stale lock that was never cleaned up
            if (entry.getValue() != 0 && curTimeInMicros > entry.getValue()) {
                if (failOnStaleLock) {
                    throw new StaleLockException("Stale lock on row '" + key + "'.  Manual cleanup requried.");
                }
                locksToDelete.add(entry.getKey());
            }
            // Lock already taken, and not by us
            else if (!entry.getKey().equals(lockColumn)) {
                throw new BusyLockException("Lock already acquired for row '" + key + "' with lock column " + entry.getKey());
            }
        }
    }

    /**
     * Release the lock by releasing this and any other stale lock columns
     */
    @Override
    public void release() throws Exception {
        if (!locksToDelete.isEmpty() || lockColumn != null) {
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
     * @throws Exception
     */
    public Map<String, Long> readLockColumns() throws Exception {
        return readLockColumns(false);
    }
    
    /**
     * Read all the lock columns.  Will also ready data columns if withDataColumns(true) was called
     * 
     * @param readDataColumns
     * @throws Exception
     */
    private Map<String, Long> readLockColumns(boolean readDataColumns) throws Exception {
        Map<String, Long> result = Maps.newLinkedHashMap();
        // Read all the columns
        if (readDataColumns) {
            columns = new OrderedColumnMap<String>();
            ColumnList<String> lockResult = keyspace
                .prepareQuery(columnFamily)
                    .setConsistencyLevel(consistencyLevel)
                    .getKey(key)
                .execute()
                    .getResult();
    
            for (Column<String> c : lockResult) {
                if (c.getName().startsWith(prefix))
                    result.put(c.getName(), readTimeoutValue(c));
                else 
                    columns.add(c);
            }
        }
        // Read only the lock columns
        else {
            ColumnList<String> lockResult = keyspace
                .prepareQuery(columnFamily)
                    .setConsistencyLevel(consistencyLevel)
                    .getKey(key)
                    .withColumnRange(new RangeBuilder().setStart(prefix + "\u0000").setEnd(prefix + "\uFFFF").build())
                .execute()
                    .getResult();

            for (Column<String> c : lockResult) {
                result.put(c.getName(), readTimeoutValue(c));
            }

        }
        return result;    
    }
    
    /**
     * Release all locks. Use this carefully as it could release a lock for a
     * running operation.
     * 
     * @return Map of previous locks
     * @throws Exception
     */
    public Map<String, Long> releaseAllLocks() throws Exception {
        return releaseLocks(true);
    }

    /**
     * Release all expired locks for this key.
     * 
     * @return map of expire locks
     * @throws Exception
     */
    public Map<String, Long> releaseExpiredLocks() throws Exception {
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
     * @return Map of locks released
     * @throws Exception
     */
    public Map<String, Long> releaseLocks(boolean force) throws Exception {
        Map<String, Long> locksToDelete = readLockColumns();

        MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(consistencyLevel);
        ColumnListMutation<String> row = m.withRow(columnFamily, key);
        long now = getCurrentTimeMicros();
        for (Entry<String, Long> c : locksToDelete.entrySet()) {
            if (force || (c.getValue() > 0 && c.getValue() < now)) {
                row.deleteColumn(c.getKey());
            }
        }
        m.execute();

        return locksToDelete;
    }

    /**
     * Get the current system time
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
    public String fillLockMutation(MutationBatch m, Long time, Integer ttl) {
        if (lockColumn != null) {
            if (!lockColumn.equals(prefix+lockId))
                throw new IllegalStateException("Can't change prefix or lockId after acquiring the lock");
        }
        else {
            lockColumn = prefix + lockId;
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
        ColumnListMutation<String> row = m.withRow(columnFamily, key);
        for (String c : locksToDelete) {
            row.deleteColumn(c);
        }
        if (!excludeCurrentLock && lockColumn != null) 
            row.deleteColumn(lockColumn);
        locksToDelete.clear();
        lockColumn = null;
    }


    public ColumnMap<String> getDataColumns() {
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

    public String getLockColumn() {
        return lockColumn;
    }
    
    public String getLockId() {
        return lockId;
    }
    
    public String getPrefix() {
        return prefix;
    }
    
    public int getRetryCount() {
        return retryCount;
    }

}

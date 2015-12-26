package com.netflix.astyanax.cql.test.recipes;

import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.cql.test.KeyspaceTests;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.recipes.locks.StaleLockException;
import com.netflix.astyanax.serializers.StringSerializer;

public class ColumnPrefixDistributedLockTest extends KeyspaceTests {

	public static ColumnFamily<String, String> CF_DIST_LOCK = ColumnFamily
            .newColumnFamily(
                    "distlock", 
                    StringSerializer.get(),
                    StringSerializer.get());

    @BeforeClass
	public static void init() throws Exception {
		initContext();
		keyspace.createColumnFamily(CF_DIST_LOCK, null);
		CF_DIST_LOCK.describe(keyspace);
    }

    @AfterClass
	public static void tearDown() throws Exception {
    	keyspace.dropColumnFamily(CF_DIST_LOCK);
    }

    @Test
    public void testLockSuccess() throws Exception {

    	ColumnPrefixDistributedRowLock<String> lock = 
    			new ColumnPrefixDistributedRowLock<String>(keyspace, CF_DIST_LOCK, "RowKeyToLock")
    			.expireLockAfter(1, TimeUnit.SECONDS)
    			.withConsistencyLevel(ConsistencyLevel.CL_ONE);

    	try {
    		lock.acquire();
    		System.out.println("Successfully acquired lock");
    	} catch (StaleLockException e) {
    		// The row contains a stale or abandoned lock
    		// These can either be manually clean up or automatically
    		// cleaned up (and ignored) by calling failOnStaleLock(false)
    		e.printStackTrace();
    		Assert.fail(e.getMessage());
    	} catch (BusyLockException e) {
    		// The row is currently locked.
    		e.printStackTrace();
    		Assert.fail(e.getMessage());
    	} finally {
    		lock.release();
    	}
    	
    	// VERIFY THAT THE LOCK ROW IS EMPTY
    	ColumnList<String> result = keyspace.prepareQuery(CF_DIST_LOCK).getRow("RowKeyToLock").execute().getResult();
    	Assert.assertTrue(result.isEmpty());
    }
    

    @Test
    public void testStaleLock() throws Exception {

    	String rowKey = "StaleRowKeyToLock";
    	String lockPrefix = "TestLockPrefix";
    	
    	Long pastTime = System.currentTimeMillis() - 2000L; 
    	Long timeInMicros = TimeUnit.MICROSECONDS.convert(pastTime, TimeUnit.MILLISECONDS);
    	
    	MutationBatch m = keyspace.prepareMutationBatch();
    	m.withRow(CF_DIST_LOCK, rowKey).putColumn(lockPrefix + "1", timeInMicros);
    	m.execute();

    	ColumnPrefixDistributedRowLock<String> lock = 
    			new ColumnPrefixDistributedRowLock<String>(keyspace, CF_DIST_LOCK, rowKey)
    			.withColumnPrefix(lockPrefix)
    			.expireLockAfter(1, TimeUnit.SECONDS)
    			.failOnStaleLock(true)
    			.withConsistencyLevel(ConsistencyLevel.CL_ONE);

    	try {
    		lock.acquire();
    		Assert.fail("Acquired lock when there was a STALE LOCK");
    		
    	} catch (StaleLockException e) {
    		// The row contains a stale or abandoned lock
    		// These can either be manually clean up or automatically
    		// cleaned up (and ignored) by calling failOnStaleLock(false)
    		System.out.println("STALE LOCK "  + e.getMessage());
    		lock.releaseExpiredLocks();
    		lock.release();
    	} 
    	
    	// VERIFY THAT THE LOCK ROW IS EMPTY
    	ColumnList<String> result = keyspace.prepareQuery(CF_DIST_LOCK).getRow(rowKey).execute().getResult();
    	Assert.assertTrue(result.isEmpty());

    }
    
    @Test
    public void testBusyLock() throws Exception {

    	String rowKey = "BusyRowKeyToLock";
    	String lockPrefix = "TestLockPrefix";
    	
    	Long futureTime = System.currentTimeMillis() + 20000L; 
    	Long timeInMicros = TimeUnit.MICROSECONDS.convert(futureTime, TimeUnit.MILLISECONDS);

    	MutationBatch m = keyspace.prepareMutationBatch();
    	m.withRow(CF_DIST_LOCK, rowKey).putColumn(lockPrefix + "1", timeInMicros);
    	m.execute();

    	ColumnPrefixDistributedRowLock<String> lock = 
    			new ColumnPrefixDistributedRowLock<String>(keyspace, CF_DIST_LOCK, rowKey)
    			.withColumnPrefix(lockPrefix)
    			.expireLockAfter(1, TimeUnit.SECONDS)
    			.failOnStaleLock(true)
    			.withConsistencyLevel(ConsistencyLevel.CL_ONE);

    	try {
    		lock.acquire();
    		Assert.fail("Acquired lock when there was a STALE LOCK");
    		
    	} catch (BusyLockException e) {
    		// The row is currently locked.
    		System.out.println("BUSY LOCK "  + e.getMessage());
    		lock.releaseAllLocks();
    	} 
    	
    	// VERIFY THAT THE LOCK ROW IS EMPTY
    	ColumnList<String> result = keyspace.prepareQuery(CF_DIST_LOCK).getRow(rowKey).execute().getResult();
    	Assert.assertTrue(result.isEmpty());
    }
}

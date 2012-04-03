package com.netflix.astyanax.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;
import com.netflix.astyanax.impl.AckingQueue;

public class MutationBatchExecutorWithQueue {
	
	private ExecutorService executor;
	private Predicate<Exception> retryablePredicate = Predicates.alwaysFalse();
	private long waitOnNoHosts = 1000;
	private int nThreads;
	private long timeout;
	private AckingQueue queue;
	private AtomicLong successCount = new AtomicLong(0);
	private AtomicLong failureCount = new AtomicLong(0);
	
	public MutationBatchExecutorWithQueue(AckingQueue queue, int nThreads) {
	    this.executor = Executors.newFixedThreadPool(
		    				nThreads, 
		    				new ThreadFactoryBuilder().setDaemon(true).build());
	    this.queue = queue;
	    this.nThreads = nThreads;
	}
	
	public MutationBatchExecutorWithQueue usingRetryablePredicate(Predicate<Exception> predicate) {
		this.retryablePredicate = predicate;
		return this;
	}
	
	public MutationBatchExecutorWithQueue startConsumers() {
		for (int i = 0; i < nThreads; i++) {
			executor.submit(new Runnable() {
				public void run() {
					MutationBatch m = null;
					while (true) {
						do {
							try {
								m = queue.getNextMutation(timeout, TimeUnit.MILLISECONDS);
								if (m != null) {
									m.execute();
									successCount.incrementAndGet();
									queue.ackMutation(m);
									m = null;
								}
							}
							catch (InterruptedException e) {
								Thread.currentThread().interrupt();
								return;
							}
							catch (Exception e) {
								failureCount.incrementAndGet();
								if (e instanceof NoAvailableHostsException) {
									try {
		                                Thread.sleep(waitOnNoHosts);
	                                } catch (InterruptedException e1) {
	    								Thread.currentThread().interrupt();
	    								return;
	                                }
	                                continue;
								}
								else {
									if (!retryablePredicate.apply(e)) {
										try {
	                                        queue.ackMutation(m);
                                        } catch (Exception e1) {
                                        	// TOOD:
                                        }
									}
									else {
										try {
	                                        queue.repushMutation(m);
                                        } catch (Exception e1) {
                                        	// TODO:
                                        }
									}
									m = null;
								}
							}
						} while (m != null);
					}
				}
			});
		}
		return this;
	}
	
	/**
	 */
    public void execute(MutationBatch m) throws Exception {
		queue.pushMutation(m);
	}
	
	public void shutdown() {
		executor.shutdown();
	}
	
	public long getFailureCount() {
		return failureCount.get();
	}
	
	public long getSuccessCount() {
		return successCount.get();
	}
}
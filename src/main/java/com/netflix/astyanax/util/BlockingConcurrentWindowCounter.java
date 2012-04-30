package com.netflix.astyanax.util;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class BlockingConcurrentWindowCounter {
    private final PriorityBlockingQueue<Integer> queue;
    private volatile int tail = 0;
    private volatile int head = 0;
    private final Semaphore semaphore;

    public BlockingConcurrentWindowCounter(int size) {
        this(size, 0);
    }

    public BlockingConcurrentWindowCounter(int size, int init) {
        this.queue = new PriorityBlockingQueue<Integer>(size);
        this.semaphore = new Semaphore(size);
        this.head = this.tail = init;
    }

    public int incrementAndGet() throws Exception {
        semaphore.acquire();
        synchronized (this) {
            return head++;
        }
    }

    public int incrementAndGet(long timeout, TimeUnit unit) throws Exception {
        semaphore.tryAcquire(timeout, unit);
        synchronized (this) {
            return head++;
        }
    }

    public synchronized int release(int index) {
        int count = 0;
        this.queue.add(index);
        while (!this.queue.isEmpty() && this.queue.peek() == tail) {
            this.queue.remove();
            tail++;
            count++;
        }
        if (count > 0)
            semaphore.release(count);
        return count;
    }
}

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

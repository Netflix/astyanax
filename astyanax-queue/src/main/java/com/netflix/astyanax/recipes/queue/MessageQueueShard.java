/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.recipes.queue;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Track the state of a partition
 * 
 * @author elandau
 */
public class MessageQueueShard implements MessageQueueShardStats {
    private volatile int   lastCount = 0;
    private final String   name;
    private final int      partition;
    private final int      shard;
    private final AtomicLong readCount = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();
    
    public MessageQueueShard(String name, int partition, int shard) {
        this.name      = name;
        this.partition = partition;
        this.shard     = shard;
    }
    
    public String getName() {
        return name;
    }
    
    public void setLastCount(int count) {
        this.lastCount = count;
        this.readCount.addAndGet(count);
    }
    
    @Override
    public long getReadCount() {
        return this.readCount.get();
    }
    
    @Override
    public long getWriteCount() {
        return this.writeCount.get();
    }
    
    @Override
    public long getLastReadCount() {
        return this.lastCount;
    }
    
    public void incInsertCount(int count) {
        this.writeCount.addAndGet(count);
    }
    
    public int getShard() {
        return this.shard;
    }
    
    public int getPartition() {
        return this.partition;
    }

    @Override
    public String toString() {
        return "Partition [lastCount=" + lastCount + ", name=" + name + ", partition=" + partition + ", shard=" + shard + "]";
    }
}
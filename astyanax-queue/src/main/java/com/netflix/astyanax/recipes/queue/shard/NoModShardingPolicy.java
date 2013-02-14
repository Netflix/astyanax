package com.netflix.astyanax.recipes.queue.shard;

import com.netflix.astyanax.recipes.queue.Message;

public class NoModShardingPolicy implements ModShardPolicy {
    private static NoModShardingPolicy instance = new NoModShardingPolicy();

    public static NoModShardingPolicy getInstance() {
        return instance;
    }
    
    @Override
    public int getMessageShard(Message message, int shardCount) {
        return 0;
    }
}

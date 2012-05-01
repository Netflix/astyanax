package com.netflix.astyanax.recipes;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.astyanax.serializers.StringSerializer;

public class Shards {
    public static interface Builder {
        Collection<ByteBuffer> build();
    }

    public static class StringShardBuilder {
        private String prefix = "";
        private int shardCount = 0;

        public StringShardBuilder setPrefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public StringShardBuilder setShardCount(int count) {
            this.shardCount = count;
            return this;
        }

        public Collection<ByteBuffer> build() {
            List<ByteBuffer> shards = Lists.newArrayListWithExpectedSize(shardCount);
            for (int i = 0; i < shardCount; i++) {
                shards.add(StringSerializer.get().toByteBuffer(prefix + i));
            }
            return shards;
        }
    }

    public static StringShardBuilder newStringShardBuilder() {
        return new StringShardBuilder();
    }
}

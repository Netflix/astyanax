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

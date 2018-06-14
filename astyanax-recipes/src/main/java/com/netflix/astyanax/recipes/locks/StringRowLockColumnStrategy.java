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
package com.netflix.astyanax.recipes.locks;

import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.util.RangeBuilder;

public class StringRowLockColumnStrategy implements LockColumnStrategy<String> {
    public static final String   DEFAULT_LOCK_PREFIX             = "_LOCK_";

    private String lockId     = null;
    private String prefix     = DEFAULT_LOCK_PREFIX;
    
    public StringRowLockColumnStrategy() {
        
    }
    
    public String getLockId() {
        return lockId;
    }

    public void setLockId(String lockId) {
        this.lockId = lockId;
    }

    public StringRowLockColumnStrategy withLockId(String lockId) {
        this.lockId = lockId;
        return this;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
    
    public StringRowLockColumnStrategy withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }
    
    @Override
    public boolean isLockColumn(String c) {
        return c.startsWith(prefix);
    }

    @Override
    public ByteBufferRange getLockColumnRange() {
        return new RangeBuilder().setStart(prefix + "\u0000").setEnd(prefix + "\uFFFF").build();
    }

    @Override
    public String generateLockColumn() {
        return prefix + lockId;
    }
}

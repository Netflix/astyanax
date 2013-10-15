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

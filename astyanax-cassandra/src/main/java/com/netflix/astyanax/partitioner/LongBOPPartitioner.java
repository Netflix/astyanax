package com.netflix.astyanax.partitioner;

import java.nio.ByteBuffer;

/**
 * This partitioner is used mostly for tests 
 * @author elandau
 *
 */
public class LongBOPPartitioner extends BigInteger127Partitioner {

    private static final LongBOPPartitioner instance = new LongBOPPartitioner();
    public static Partitioner get() {
        return instance;
    }

    protected LongBOPPartitioner() {
        
    }
    
    @Override
    public String getTokenForKey(ByteBuffer key) {
        return Long.toString(key.duplicate().asLongBuffer().get());
    }
}
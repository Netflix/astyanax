package com.netflix.astyanax.partitioner;

import java.nio.ByteBuffer;

import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;

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

    @Override
    public RingPosition getRingPositionForKey(ByteBuffer key) {
        return new TokenRingPosition(new LongToken(key.duplicate().asLongBuffer().get()));
    }
}
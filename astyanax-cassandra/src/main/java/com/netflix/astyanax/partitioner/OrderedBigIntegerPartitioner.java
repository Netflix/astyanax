package com.netflix.astyanax.partitioner;

import java.nio.ByteBuffer;

import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;

import com.netflix.astyanax.serializers.BigIntegerSerializer;

public class OrderedBigIntegerPartitioner extends BigInteger127Partitioner {

    private static final OrderedBigIntegerPartitioner instance = new OrderedBigIntegerPartitioner();
    public static Partitioner get() {
        return instance;
    }

    protected OrderedBigIntegerPartitioner() {
        
    }
    
    @Override
    public String getTokenForKey(ByteBuffer key) {
        return BigIntegerSerializer.get().fromByteBuffer(key).toString();
    }

    @Override
    public RingPosition getRingPositionForKey(ByteBuffer key) {
        return new TokenRingPosition(new BigIntegerToken(BigIntegerSerializer.get().fromByteBuffer(key)));
    }
}
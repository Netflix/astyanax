package com.netflix.astyanax.partitioner;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.impl.TokenRangeImpl;

public class Murmur3Partitioner implements Partitioner {
    public static final BigInteger MINIMUM = new BigInteger(Long.toString(Long.MIN_VALUE));
    public static final BigInteger MAXIMUM = new BigInteger(Long.toString(Long.MAX_VALUE));

    public static final BigInteger ONE     = new BigInteger("1");
    
    private static final org.apache.cassandra.dht.Murmur3Partitioner partitioner = new org.apache.cassandra.dht.Murmur3Partitioner();
    private static final Murmur3Partitioner instance = new Murmur3Partitioner();
    
    public static Partitioner get() {
        return instance;
    }
    
    private Murmur3Partitioner() {
        
    }
    
    @Override
    public String getMinToken() {
        return MINIMUM.toString();
    }

    @Override
    public String getMaxToken() {
        return MAXIMUM.toString();
    }

    @Override
    public List<TokenRange> splitTokenRange(String first, String last, int count) {
        if (first.equals(last)) {
            last = getTokenMinusOne(last);
        }
        
        List<TokenRange> tokens = Lists.newArrayList();
        List<String> splits = splitRange(new BigInteger(first), new BigInteger(last), count);
        Iterator<String> iter = splits.iterator();
        String current = iter.next();
        while (iter.hasNext()) {
            String next = iter.next();
            tokens.add(new TokenRangeImpl(current, next, new ArrayList<String>()));
            current = next;
        }
        return tokens;
    }

    @Override
    public List<TokenRange> splitTokenRange(int count) {
        return splitTokenRange(getMinToken(), getMaxToken(), count);
    }

    @Override
    public String getTokenForKey(ByteBuffer key) {
        return partitioner.getToken(key).toString();
    }
    
    public <T> String getTokenForKey(T key, Serializer<T> serializer) {
        return partitioner.getToken(serializer.toByteBuffer(key)).toString();
    }

    @Override
    public String getTokenMinusOne(String token) {
        Long lToken = Long.parseLong(token);
        // if zero rotate to the Maximum else minus one.
        if (lToken.equals(MINIMUM))
            return MAXIMUM.toString();
        else
            return Long.toString(lToken - 1);
    }

    public static List<String> splitRange(BigInteger first, BigInteger last, int count) {
        List<String> tokens = Lists.newArrayList();
        tokens.add(first.toString());
        BigInteger delta = (last.subtract(first).divide(BigInteger.valueOf((long)count)));
        BigInteger current = first;
        for (int i = 0; i < count-1; i++) {
            current = current.add(delta);
            tokens.add(current.toString());
        }
        tokens.add(last.toString());
        return tokens;
    }

}

package com.netflix.astyanax.partitioner;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.impl.TokenRangeImpl;

public class Murmur3Partitioner implements Partitioner {
    public static final Long MINIMUM = Long.MIN_VALUE;
    public static final Long MAXIMUM = Long.MAX_VALUE;
    public static final Long ONE     = 1L;
    
    private static final org.apache.cassandra.dht.Murmur3Partitioner partitioner = new org.apache.cassandra.dht.Murmur3Partitioner();
    private static final Murmur3Partitioner instance = new Murmur3Partitioner();
    
    public static Partitioner get() {
        return instance;
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
        for (int i = 0; i < count; i++) {
            String startToken = getSegmentToken(count, i, new BigInteger(first), new BigInteger(last));
            String endToken;
            if (i == count-1 && last.equals(getMaxToken())) 
                endToken = getMinToken();
            else
                endToken = getSegmentToken(count, i+1, new BigInteger(first), new BigInteger(last));
            tokens.add(new TokenRangeImpl(startToken, endToken, new ArrayList<String>()));
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

    public static String getSegmentToken(int size, int position, BigInteger minInitialToken, BigInteger maxInitialToken ) {
        BigInteger decValue = minInitialToken;
        if (position != 0)
            decValue = maxInitialToken.multiply(new BigInteger("" + position)).divide(new BigInteger("" + size));
        return decValue.toString();
    }

}

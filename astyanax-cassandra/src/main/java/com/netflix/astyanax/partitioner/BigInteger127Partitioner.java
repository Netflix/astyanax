package com.netflix.astyanax.partitioner;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.dht.RandomPartitioner;

import com.google.common.collect.Lists;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.impl.TokenRangeImpl;

public class BigInteger127Partitioner implements Partitioner {

    public static final BigInteger MINIMUM = new BigInteger("" + 0);
    public static final BigInteger MAXIMUM = new BigInteger("" + 2).pow(127).subtract(new BigInteger("1"));
    public static final BigInteger ONE     = new BigInteger("1");
    
    private static final RandomPartitioner partitioner = new RandomPartitioner();
    private static final BigInteger127Partitioner instance = new BigInteger127Partitioner();
    
    public static Partitioner get() {
        return instance;
    }
    
    public BigInteger127Partitioner() {
        
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
            first = getMinToken();
            last  = getMaxToken();
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
        BigInteger bigInt = new BigInteger(token);
        // if zero rotate to the Maximum else minus one.
        if (bigInt.equals(MINIMUM))
            return MAXIMUM.toString();
        else
            return bigInt.subtract(ONE).toString();
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

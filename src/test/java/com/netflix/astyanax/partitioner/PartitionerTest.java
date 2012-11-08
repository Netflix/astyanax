package com.netflix.astyanax.partitioner;

import java.util.List;

import org.junit.Test;

import com.netflix.astyanax.connectionpool.TokenRange;

public class PartitionerTest {
    @Test
    public void testSplit() {
        BigInteger127Partitioner partitioner = new BigInteger127Partitioner();
        List<TokenRange> ranges = partitioner.splitTokenRange(4);
        
        for (TokenRange range : ranges) {
            System.out.println(range);
        }
    }
    
    @Test
    public void testSplitWithStartEnd() {
        BigInteger127Partitioner partitioner = new BigInteger127Partitioner();
        List<TokenRange> ranges = partitioner.splitTokenRange(BigInteger127Partitioner.MINIMUM.toString(), BigInteger127Partitioner.MAXIMUM.toString(), 4);
        
        for (TokenRange range : ranges) {
            System.out.println(range);
        }
    }
    
    @Test
    public void testSplitWithZeros() {
        BigInteger127Partitioner partitioner = new BigInteger127Partitioner();
        List<TokenRange> ranges = partitioner.splitTokenRange("0", "0", 4);
        
        for (TokenRange range : ranges) {
            System.out.println(range);
        }
    }
}   

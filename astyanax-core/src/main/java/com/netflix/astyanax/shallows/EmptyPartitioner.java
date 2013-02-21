package com.netflix.astyanax.shallows;

import java.nio.ByteBuffer;
import java.util.List;

import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.partitioner.Partitioner;

public class EmptyPartitioner implements Partitioner {

    @Override
    public String getMinToken() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getMaxToken() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTokenMinusOne(String token) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<TokenRange> splitTokenRange(String first, String last, int count) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<TokenRange> splitTokenRange(int count) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTokenForKey(ByteBuffer key) {
        // TODO Auto-generated method stub
        return null;
    }

}

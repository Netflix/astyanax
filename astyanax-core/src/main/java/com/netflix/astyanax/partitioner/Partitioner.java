package com.netflix.astyanax.partitioner;

import java.nio.ByteBuffer;
import java.util.List;

import com.netflix.astyanax.connectionpool.TokenRange;

/**
 * Base interface for token partitioning utilities
 * 
 * @author elandau
 *
 */
public interface Partitioner {
    /**
     * @return Return the smallest token in the token space
     */
    String getMinToken();
    
    /**
     * @return Return the largest token in the token space
     */
    String getMaxToken();
    
    /**
     * @return Return the token immediately before this one
     */
    String getTokenMinusOne(String token);
    
    /**
     * Split the token range into N equal size segments and return the start token
     * of each segment
     * 
     * @param first
     * @param last
     * @param count
     */
    List<TokenRange> splitTokenRange(String first, String last, int count);
    
    /**
     * Split the entire token range into 'count' equal size segments
     * @param count
     */
    List<TokenRange> splitTokenRange(int count);
    
    /**
     * Return the token for the specifie key
     * @param key
     */
    String getTokenForKey(ByteBuffer key);
    
}

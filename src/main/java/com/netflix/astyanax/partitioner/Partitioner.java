package com.netflix.astyanax.partitioner;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.dht.RingPosition;

import com.netflix.astyanax.connectionpool.TokenRange;

/**
 * Base interface for token partitioning utilities
 * 
 * @author elandau
 *
 */
public interface Partitioner {
    /**
     * Return the smallest token in the token space
     * @return
     */
    String getMinToken();
    
    /**
     * Return the largest token in the token space
     * @return
     */
    String getMaxToken();
    
    /**
     * Return the token immediately before this one
     */
    String getTokenMinusOne(String token);
    
    /**
     * Split the token range into N equal size segments and return the start token
     * of each segment
     * 
     * @param first
     * @param last
     * @param count
     * @return
     */
    List<TokenRange> splitTokenRange(String first, String last, int count);
    
    /**
     * Split the entire token range into 'count' equal size segments
     * @param count
     * @return
     */
    List<TokenRange> splitTokenRange(int count);
    
    /**
     * Return the token for the specified key
     * @param key
     * @return
     */
    String getTokenForKey(ByteBuffer key);

    /**
     * Return the ring position for the specified key
     * @param key
     * @return
     */
    RingPosition getRingPositionForKey(ByteBuffer key);

    /**
     * Return the ring position for the specified token
     * @param token
     * @return
     */
    RingPosition getRingPositionForToken(String token);
}

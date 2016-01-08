package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;
import com.netflix.astyanax.partitioner.RingPosition;

/**
 * Collection of pools that own a token range of the ring
 * 
 * @author elandau
 *
 * @param <CL>
 */
public class TokenHostConnectionPoolPartition<CL> extends HostConnectionPoolPartition<CL> {

    private final RingPosition token;

    public TokenHostConnectionPoolPartition(RingPosition id, LatencyScoreStrategy strategy) {
        super(strategy);
        this.token = id;
    }
    /**
     * Token or shard identifying this partition.
     * 
     * @return
     */
    public RingPosition id() {
        return token;
    }
}

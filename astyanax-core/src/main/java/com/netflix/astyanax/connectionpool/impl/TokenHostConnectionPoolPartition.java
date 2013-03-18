package com.netflix.astyanax.connectionpool.impl;

import java.math.BigInteger;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;

/**
 * Collection of pools that own a token range of the ring
 * 
 * @author elandau
 *
 * @param <CL>
 */
public class TokenHostConnectionPoolPartition<CL> extends HostConnectionPoolPartition<CL> {

    private final BigInteger  token;

    public TokenHostConnectionPoolPartition(BigInteger id, LatencyScoreStrategy strategy) {
        super(strategy);
        this.token = id;
    }
    /**
     * Token or shard identifying this partition.
     * 
     * @return
     */
    public BigInteger id() {
        return token;
    }
}

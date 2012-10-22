package com.netflix.astyanax.impl;

import java.util.List;
import java.util.Map;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * HostSupplier that uses existing hosts in the connection pool to execute a ring
 * describe and get the entire list of hosts and their tokens from Cassandra.
 * 
 * @author elandau
 *
 */
public class RingDescribeHostSupplier implements Supplier<List<Host>> {
    private final Keyspace  keyspace;
    private final int       defaultPort;
    private final String    dc;
    private final String    rack;
    
    public RingDescribeHostSupplier(Keyspace keyspace, int defaultPort, String dc, String rack) {
        this.keyspace    = keyspace;
        this.defaultPort = defaultPort;
        this.dc          = dc;
        this.rack        = rack;
    }

    public RingDescribeHostSupplier(Keyspace keyspace, int defaultPort, String dc) {
        this(keyspace, defaultPort, dc, null);
    }
    
    public RingDescribeHostSupplier(Keyspace keyspace, int defaultPort) {
        this(keyspace, defaultPort, null, null);
    }

    @Override
    public List<Host> get() {
        try {
            Map<String, Host> ipToHost = Maps.newHashMap();

            for (TokenRange range : keyspace.describeRing(dc, rack)) {
                for (String endpoint : range.getEndpoints()) {
                    Host host = ipToHost.get(endpoint);
                    if (host == null) {
                        host = new Host(endpoint, defaultPort);
                        ipToHost.put(endpoint, host);
                    }
                    
                    host.getTokenRanges().add(range);
                }
            }
            return Lists.newArrayList(ipToHost.values());
        }
        catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

}

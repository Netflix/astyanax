package com.netflix.astyanax.impl;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
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
public class RingDescribeHostSupplier implements Supplier<Map<BigInteger, List<Host>>> {
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
    public Map<BigInteger, List<Host>> get() {
        try {
            Map<BigInteger, List<Host>> hosts = Maps.newLinkedHashMap();

            for (TokenRange range : keyspace.describeRing(dc, rack)) {
                hosts.put(new BigInteger(range.getEndToken()),
                        Lists.transform(range.getEndpoints(), new Function<String, Host>() {
                            @Override
                            public Host apply(String ip) {
                                return new Host(ip, defaultPort);
                            }
                        }));
            }
            return hosts;
        }
        catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

}

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

public class RingDescribeHostSupplier implements
        Supplier<Map<BigInteger, List<Host>>> {
    private final Keyspace keyspace;
    private final int defaultPort;

    public RingDescribeHostSupplier(Keyspace keyspace, int defaultPort) {
        this.keyspace = keyspace;
        this.defaultPort = defaultPort;
    }

    @Override
    public Map<BigInteger, List<Host>> get() {
        try {
            Map<BigInteger, List<Host>> hosts = Maps.newLinkedHashMap();

            for (TokenRange range : keyspace.describeRing()) {
                hosts.put(new BigInteger(range.getStartToken()), Lists
                        .transform(range.getEndpoints(),
                                new Function<String, Host>() {
                                    @Override
                                    public Host apply(String ip) {
                                        return new Host(ip, defaultPort);
                                    }
                                }));
            }
            return hosts;
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

}

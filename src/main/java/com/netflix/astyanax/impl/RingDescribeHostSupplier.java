package com.netflix.astyanax.impl;

import java.util.List;
import java.util.Map;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;

public class RingDescribeHostSupplier implements Supplier<Map<Token, List<Host>>> {
    private final Keyspace keyspace;
    private final int defaultPort;
    private final Supplier<IPartitioner> partitioner;
    private final Predicate<TokenRange.EndpointDetails> endPointFilter;

    public RingDescribeHostSupplier(Keyspace keyspace, int defaultPort, Supplier<IPartitioner> partitioner,
                                    Predicate<TokenRange.EndpointDetails> endPointFilter) {
        this.keyspace = keyspace;
        this.defaultPort = defaultPort;
        this.partitioner = partitioner;
        this.endPointFilter = endPointFilter;
    }

    @Override
    public Map<Token, List<Host>> get() {
        try {
            Map<Token, List<Host>> hosts = Maps.newLinkedHashMap();

            for (TokenRange range : keyspace.describeRing()) {
                Token.TokenFactory tokenFactory = partitioner.get().getTokenFactory();
                Token token = tokenFactory.fromString(range.getEndToken());

                List<Host> endpoints = Lists.newArrayList();
                for (TokenRange.EndpointDetails endpoint : range.getEndpointDetails()) {
                    if (endPointFilter.apply(endpoint)) {
                        endpoints.add(new Host(endpoint.getHost(), defaultPort));
                    }
                }

                if (!endpoints.isEmpty()) {
                    hosts.put(token, endpoints);
                }
            }

            return hosts;
        }
        catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

}

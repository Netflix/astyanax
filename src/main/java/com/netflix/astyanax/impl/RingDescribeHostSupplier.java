package com.netflix.astyanax.impl;

import java.util.List;
import java.util.Map;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class RingDescribeHostSupplier implements Supplier<Map<Token, List<Host>>> {
    private final Keyspace keyspace;
    private final int defaultPort;
    private final Supplier<IPartitioner> partitioner;

    public RingDescribeHostSupplier(Keyspace keyspace, int defaultPort, Supplier<IPartitioner> partitioner) {
        this.keyspace = keyspace;
        this.defaultPort = defaultPort;
        this.partitioner = partitioner;
    }

    @Override
    public Map<Token, List<Host>> get() {
        try {
            Map<Token, List<Host>> hosts = Maps.newLinkedHashMap();

            for (TokenRange range : keyspace.describeRing()) {
                Token.TokenFactory tokenFactory = partitioner.get().getTokenFactory();
                hosts.put(tokenFactory.fromString(range.getEndToken()),
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

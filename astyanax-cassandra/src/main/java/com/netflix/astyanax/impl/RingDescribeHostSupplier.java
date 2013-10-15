/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.impl;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final static Logger LOG = LoggerFactory.getLogger(RingDescribeHostSupplier.class);
    
    private final Keyspace  keyspace;
    private final int       defaultPort;
    private final String    dc;
    private final String    rack;
    private volatile List<Host> previousHosts;
    
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
    public synchronized List<Host> get() {
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
            
            previousHosts = Lists.newArrayList(ipToHost.values());
            return previousHosts;
        }
        catch (ConnectionException e) {
            if (previousHosts == null) {
                throw new RuntimeException(e);
            }
            LOG.warn("Failed to get hosts from " + keyspace.getKeyspaceName() + " via ring describe.  Will use previously known ring instead");
            return previousHosts;
        }
    }

}

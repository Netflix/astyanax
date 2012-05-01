package com.netflix.astyanax.connectionpool;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * MBean monitoring for a connection pool
 * 
 * @author elandau
 * 
 */
public class JmxConnectionPoolMonitor implements JmxConnectionPoolMonitorMBean {
    private final ConnectionPool<?> pool;

    private final static int DEFAULT_PORT = 7102;

    public JmxConnectionPoolMonitor(ConnectionPool<?> pool) {
        this.pool = pool;
    }

    @Override
    public boolean addHost(String host) {
        return pool.addHost(new Host(host, DEFAULT_PORT), true);
    }

    @Override
    public boolean removeHost(String host) {
        return pool.addHost(new Host(host, DEFAULT_PORT), true);
    }

    @Override
    public boolean isHostUp(String host) {
        return pool.isHostUp(new Host(host, DEFAULT_PORT));
    }

    @Override
    public boolean hasHost(String host) {
        return pool.hasHost(new Host(host, DEFAULT_PORT));
    }

    @Override
    public String getActiveHosts() {
        return StringUtils.join(Lists.transform(pool.getActivePools(), new Function<HostConnectionPool<?>, String>() {
            @Override
            public String apply(HostConnectionPool<?> host) {
                return host.getHost().getName();
            }
        }), ",");
    }
}

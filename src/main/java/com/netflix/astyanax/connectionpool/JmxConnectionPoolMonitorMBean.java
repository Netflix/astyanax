package com.netflix.astyanax.connectionpool;

public interface JmxConnectionPoolMonitorMBean {
    boolean addHost(String host);

    boolean removeHost(String host);

    boolean isHostUp(String host);

    boolean hasHost(String host);

    String getActiveHosts();
}

package com.netflix.astyanax.connectionpool;

public interface NodeDiscoveryMonitorMBean {
    long getRefreshCount();

    long getErrorCount();

    String getLastException();

    String getLastRefreshTime();

    String getRawHostList();
}

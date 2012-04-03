package com.netflix.astyanax.connectionpool;

public class NodeDiscoveryMonitor implements NodeDiscoveryMonitorMBean {

    private NodeDiscovery discovery;

    public NodeDiscoveryMonitor(NodeDiscovery discovery) {
        this.discovery = discovery;
    }

    @Override
    public long getRefreshCount() {
        return discovery.getRefreshCount();
    }

    @Override
    public long getErrorCount() {
        return discovery.getErrorCount();
    }

    @Override
    public String getLastException() {
        Exception e = discovery.getLastException();
        return (e != null) ? e.getMessage() : "none";
    }

    @Override
    public String getLastRefreshTime() {
        return discovery.getLastRefreshTime().toString();
    }

    @Override
    public String getRawHostList() {
        return discovery.getRawHostList();
    }
}

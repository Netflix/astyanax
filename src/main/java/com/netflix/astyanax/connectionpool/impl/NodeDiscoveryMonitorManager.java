package com.netflix.astyanax.connectionpool.impl;

import java.lang.management.ManagementFactory;
import java.util.HashMap;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.NodeDiscovery;
import com.netflix.astyanax.connectionpool.NodeDiscoveryMonitor;
import com.netflix.astyanax.connectionpool.NodeDiscoveryMonitorMBean;

public class NodeDiscoveryMonitorManager {
    private MBeanServer mbs;

    private static NodeDiscoveryMonitorManager monitorInstance;

    private HashMap<String, NodeDiscoveryMonitorMBean> monitors;

    private NodeDiscoveryMonitorManager() {
        mbs = ManagementFactory.getPlatformMBeanServer();
        monitors = Maps.newHashMap();
    }

    public static NodeDiscoveryMonitorManager getInstance() {
        if (monitorInstance == null) {
            monitorInstance = new NodeDiscoveryMonitorManager();
        }
        return monitorInstance;
    }

    public synchronized void registerMonitor(String monitorName,
            NodeDiscovery discovery) {

        monitorName = generateMonitorName(monitorName);

        if (!monitors.containsKey(monitorName)) {
            NodeDiscoveryMonitorMBean mbean;
            try {
                ObjectName oName = new ObjectName(monitorName);
                mbean = new NodeDiscoveryMonitor(discovery);
                monitors.put(monitorName, mbean);
                mbs.registerMBean(mbean, oName);

            } catch (Exception e) {
                monitors.remove(monitorName);
            }
        }
    }

    public synchronized void unregisterMonitor(String monitorName,
            NodeDiscovery discovery) {
        monitorName = generateMonitorName(monitorName);
        monitors.remove(monitorName);
        try {
            mbs.unregisterMBean(new ObjectName(monitorName));
        } catch (Exception e) {
        }
    }

    public synchronized NodeDiscoveryMonitorMBean getCassandraMonitor(
            String monitorName) {
        monitorName = generateMonitorName(monitorName);
        return monitors.get(monitorName);
    }

    private String generateMonitorName(String monitorName) {

        StringBuilder sb = new StringBuilder();
        sb.append("com.netflix.MonitoredResources");
        sb.append(":type=ASTYANAX");
        sb.append(",name=" + monitorName.toString());
        sb.append(",ServiceType=discovery");
        return sb.toString();
    }
}

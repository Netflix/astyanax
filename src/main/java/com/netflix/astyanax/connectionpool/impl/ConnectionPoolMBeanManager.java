package com.netflix.astyanax.connectionpool.impl;

import java.lang.management.ManagementFactory;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.JmxConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.JmxConnectionPoolMonitorMBean;

public class ConnectionPoolMBeanManager {
    private static final Logger LOG = LoggerFactory
            .getLogger(ConnectionPoolMBeanManager.class);

    private final MBeanServer mbs;

    private static ConnectionPoolMBeanManager monitorInstance;

    private final Map<String, JmxConnectionPoolMonitorMBean> monitors;

    private ConnectionPoolMBeanManager() {
        mbs = ManagementFactory.getPlatformMBeanServer();
        monitors = Maps.newHashMap();
    }

    public static synchronized ConnectionPoolMBeanManager getInstance() {
        if (monitorInstance == null) {
            monitorInstance = new ConnectionPoolMBeanManager();
        }
        return monitorInstance;
    }

    public synchronized void registerMonitor(String name, ConnectionPool<?> pool) {

        String monitorName = generateMonitorName(name);

        if (!monitors.containsKey(monitorName)) {
            JmxConnectionPoolMonitorMBean mbean;
            try {
                LOG.info("Registering mbean: {}", monitorName);
                ObjectName oName = new ObjectName(monitorName);
                mbean = new JmxConnectionPoolMonitor(pool);
                monitors.put(monitorName, mbean);
                mbs.registerMBean(mbean, oName);

            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                monitors.remove(monitorName);
            }
        }
    }

    public synchronized void unregisterMonitor(String name,
            ConnectionPool<?> pool) {
        String monitorName = generateMonitorName(name);
        monitors.remove(monitorName);
        try {
            mbs.unregisterMBean(new ObjectName(monitorName));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public synchronized JmxConnectionPoolMonitorMBean getCassandraMonitor(
            String name) {
        String monitorName = generateMonitorName(name);
        return monitors.get(monitorName);
    }

    private String generateMonitorName(String name) {
        StringBuilder sb = new StringBuilder();
        sb.append("com.netflix.MonitoredResources");
        sb.append(":type=ASTYANAX");
        sb.append(",name=").append(name);
        sb.append(",ServiceType=connectionpool");
        return sb.toString();
    }

}

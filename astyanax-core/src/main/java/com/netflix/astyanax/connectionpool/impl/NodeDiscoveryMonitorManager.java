/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.connectionpool.impl;

import java.lang.management.ManagementFactory;
import java.util.HashMap;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.collect.Maps;
import com.netflix.astyanax.connectionpool.NodeDiscovery;
import com.netflix.astyanax.connectionpool.NodeDiscoveryMonitor;
import com.netflix.astyanax.connectionpool.NodeDiscoveryMonitorMBean;

/**
 * Jmx bean monitor manager for {@link NodeDiscoveryMonitor}
 * @author elandau
 *
 */
public class NodeDiscoveryMonitorManager {
    private MBeanServer mbs;

    private static class LazyHolder {
        private static final NodeDiscoveryMonitorManager instance = new NodeDiscoveryMonitorManager();
    }
    
    private HashMap<String, NodeDiscoveryMonitorMBean> monitors;

    private NodeDiscoveryMonitorManager() {
        mbs = ManagementFactory.getPlatformMBeanServer();
        monitors = Maps.newHashMap();
    }

    public static NodeDiscoveryMonitorManager getInstance() {
        return LazyHolder.instance;
    }

    public synchronized void registerMonitor(String monitorName, NodeDiscovery discovery) {

        monitorName = generateMonitorName(monitorName);

        if (!monitors.containsKey(monitorName)) {
            NodeDiscoveryMonitorMBean mbean;
            try {
                ObjectName oName = new ObjectName(monitorName);
                mbean = new NodeDiscoveryMonitor(discovery);
                monitors.put(monitorName, mbean);
                mbs.registerMBean(mbean, oName);

            }
            catch (Exception e) {
                monitors.remove(monitorName);
            }
        }
    }

    public synchronized void unregisterMonitor(String monitorName, NodeDiscovery discovery) {
        monitorName = generateMonitorName(monitorName);
        monitors.remove(monitorName);
        try {
            mbs.unregisterMBean(new ObjectName(monitorName));
        }
        catch (Exception e) {
        }
    }

    public synchronized NodeDiscoveryMonitorMBean getCassandraMonitor(String monitorName) {
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

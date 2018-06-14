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

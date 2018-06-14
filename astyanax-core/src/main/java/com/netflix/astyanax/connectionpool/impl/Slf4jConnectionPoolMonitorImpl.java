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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.HostDownException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;

/**
 * 
 * Impl for {@link CountingConnectionPoolMonitor} that does not track any internal state / counters. It simply uses a logger to log out the events. 
 * Useful for debugging and basic event tracking. 
 * 
 * @author elandau
 *
 */
public class Slf4jConnectionPoolMonitorImpl extends CountingConnectionPoolMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(Slf4jConnectionPoolMonitorImpl.class);

    @Override
    public void incOperationFailure(Host host, Exception reason) {
        if (reason instanceof NotFoundException) {
            // Ignore this
            return;
        }

        super.incOperationFailure(host, reason);
        LOG.warn(reason.getMessage());
    }

    @Override
    public void incConnectionClosed(Host host, Exception reason) {
        super.incConnectionClosed(host, reason);

        if (reason != null) {
            LOG.info(reason.getMessage());
        }
    }

    @Override
    public void incConnectionCreateFailed(Host host, Exception reason) {
        super.incConnectionCreateFailed(host, reason);
        if (reason != null) {
            LOG.info(reason.getMessage());
        }
    }

    @Override
    public void incFailover(Host host, Exception reason) {
        if (reason != null) {
            if (reason instanceof HostDownException || reason instanceof PoolTimeoutException) {
                // we don't need to log these
            }
            else {
                LOG.warn(reason.getMessage());
            }
        }
        super.incFailover(host, reason);
    }

    @Override
    public void onHostAdded(Host host, HostConnectionPool<?> pool) {
        super.onHostAdded(host, pool);
        LOG.info("HostAdded: " + host);
    }

    @Override
    public void onHostRemoved(Host host) {
        super.onHostRemoved(host);
        LOG.info("HostRemoved: " + host);
    }

    @Override
    public void onHostDown(Host host, Exception reason) {
        super.onHostDown(host, reason);
        LOG.info("HostDown: " + host);
    }

    @Override
    public void onHostReactivated(Host host, HostConnectionPool<?> pool) {
        super.onHostReactivated(host, pool);
        LOG.info("HostReactivated: " + host);
    }
}

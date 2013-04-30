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
package com.netflix.astyanax.shallows;

import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.HostStats;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmptyConnectionPoolMonitor implements ConnectionPoolMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmptyConnectionPoolMonitor.class);

    private static EmptyConnectionPoolMonitor instance = new EmptyConnectionPoolMonitor();

    public static EmptyConnectionPoolMonitor getInstance() {
        return instance;
    }

    private EmptyConnectionPoolMonitor() {

    }

    public String toString() {
        return "EmptyConnectionPoolMonitor[]";
    }

    @Override
    public void incOperationSuccess(Host host, long latency) {
    }

    @Override
    public void incConnectionBorrowed(Host host, long delay) {
    }

    @Override
    public void incConnectionReturned(Host host) {
    }

    @Override
    public void onHostAdded(Host host, HostConnectionPool<?> pool) {
        LOGGER.info(String.format("Added host " + host));
    }

    @Override
    public void onHostRemoved(Host host) {
        LOGGER.info(String.format("Remove host " + host));
    }

    @Override
    public void onHostDown(Host host, Exception reason) {
        LOGGER.warn(String.format("Downed host " + host + " reason=\"" + reason + "\""));
    }

    @Override
    public void onHostReactivated(Host host, HostConnectionPool<?> pool) {
        LOGGER.info(String.format("Reactivating host " + host));
    }

    @Override
    public void incFailover(Host host, Exception e) {
    }

    @Override
    public void incConnectionCreated(Host host) {
    }

    @Override
    public void incConnectionCreateFailed(Host host, Exception e) {
    }

    @Override
    public void incOperationFailure(Host host, Exception e) {
    }

    @Override
    public void incConnectionClosed(Host host, Exception e) {
    }

    @Override
    public Map<Host, HostStats> getHostStats() {
        return null;
    }

    @Override
    public long getOperationFailureCount() {
        return 0;
    }

    @Override
    public long getOperationSuccessCount() {
        return 0;
    }

    @Override
    public long getConnectionCreatedCount() {
        return 0;
    }

    @Override
    public long getConnectionClosedCount() {
        return 0;
    }

    @Override
    public long getConnectionCreateFailedCount() {
        return 0;
    }

    @Override
    public long getConnectionBorrowedCount() {
        return 0;
    }

    @Override
    public long getConnectionReturnedCount() {
        return 0;
    }

    @Override
    public long getPoolExhaustedTimeoutCount() {
        return 0;
    }

    @Override
    public long getOperationTimeoutCount() {
        return 0;
    }

    @Override
    public long getFailoverCount() {
        return 0;
    }

    @Override
    public long getNoHostCount() {
        return 0;
    }

    @Override
    public long getSocketTimeoutCount() {
        return 0;
    }

    @Override
    public long getUnknownErrorCount() {
        return 0;
    }

    @Override
    public long getBadRequestCount() {
        return 0;
    }

    @Override
    public long notFoundCount() {
        return 0;
    }

    @Override
    public long getInterruptedCount() {
        return 0;
    }

    @Override
    public long getHostCount() {
        return 0;
    }

    @Override
    public long getHostAddedCount() {
        return 0;
    }

    @Override
    public long getHostRemovedCount() {
        return 0;
    }

    @Override
    public long getHostDownCount() {
        return 0;
    }

    @Override
    public long getTransportErrorCount() {
        return 0;
    }

    @Override
    public long getHostActiveCount() {
        return 0;
    }
}

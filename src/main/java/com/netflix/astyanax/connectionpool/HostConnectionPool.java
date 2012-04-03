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
package com.netflix.astyanax.connectionpool;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Pool of connections for a single host
 * 
 * @author elandau
 * 
 * @param <CL>
 */
public interface HostConnectionPool<CL> {
    /**
     * Borrow a connection from the host. May create a new connection if one is
     * not available.
     * 
     * @param timeout
     * @return
     * @throws ConnectionException
     */
    Connection<CL> borrowConnection(int timeout) throws ConnectionException;

    /**
     * This open is different from borrowConnection in that it actually creates
     * a new connection without waiting for one that may be idle. openConnection
     * is still subject to all other connection pool limitations.
     * 
     * @return
     * @throws ConnectionException
     */
    Connection<CL> openConnection() throws ConnectionException;

    /**
     * Return a connection to the host's pool. May close the connection if the
     * pool is down or the last exception on the connection is determined to be
     * fatal.
     * 
     * @param connection
     * @return True if connection was closed
     */
    boolean returnConnection(Connection<CL> connection);

    /**
     * Close this connection and update internal state
     * 
     * @param connection
     * @return
     */
    boolean closeConnection(Connection<CL> connection);

    /**
     * Shut down the host so no more connections may be created when
     * borrowConnections is called and connections will be terminated when
     * returnConnection is called.
     */
    void markAsDown(ConnectionException reason);

    /**
     * Completely shut down this connection pool as part of a client shutdown
     */
    void shutdown();

    /**
     * Create numConnections new connections and add them to the
     * 
     * @throws ConnectionException
     * @throws InterruptedException
     * @returns Actual number of connections created
     */
    int growConnections(int numConnections) throws ConnectionException,
            InterruptedException;

    /**
     * Get the host to which this pool is associated
     * 
     * @return
     */
    Host getHost();

    /**
     * Get number of open connections including any that are currently borrowed
     * and those that are currently idel
     * 
     * @return
     */
    int getActiveConnectionCount();

    /**
     * Get the number of pending connection open attempts
     * 
     * @return
     */
    int getPendingConnectionCount();

    /**
     * Get number of threads blocked waiting for a free connection
     * 
     * @return
     */
    int getBlockedThreadCount();

    /**
     * Return the number of idle active connections. These are connections that
     * can be borrowed immediatley without having to make a new connection to
     * the remote server.
     * 
     * @return
     */
    int getIdleConnectionCount();

    /**
     * Get number of currently borrowed connections
     * 
     * @return
     */
    int getBusyConnectionCount();

    /**
     * Determine if pool is shut down.
     * 
     * @return
     */
    boolean isShutdown();

    /**
     * Return implementation specific score to be used by weighted pool
     * selection algorithms
     * 
     * @return
     */
    double getScore();

    /**
     * Get the average latency as calculated by the scoring strategy
     * 
     * @return
     */
    double getMeanLatency();

    /**
     * Add a single latency sample after an operation on a connection belonging
     * to this pool
     * 
     * @param lastLatency
     */
    void addLatencySample(long lastLatency, long now);

}

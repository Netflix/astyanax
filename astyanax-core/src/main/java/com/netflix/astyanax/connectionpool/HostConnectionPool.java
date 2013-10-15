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
import com.netflix.astyanax.connectionpool.impl.SimpleHostConnectionPool;

/**
 * Interface for a pool of {@link Connection}(s) for a single {@link Host}
 * 
 * The interface prescribes certain key features required by clients of this class, such as 
 *      <ol>
 *      <li> Basic connection pool life cycle management such as prime connections (init) and shutdown </li> <br/>
 *      
 *      <li> Basic {@link Connection} life cycle management such as borrow / return / close / markAsDown </li> <br/>
 *      
 *      <li> Tracking the {@link Host} associated with the connection pool. </li> <br/>
 *      
 *      <li> Visibility into the status of the connection pool and it's connections.
 *         <ol>
 *         <li>  Tracking status of pool -  isConnecting / isActive  / isShutdown  </li>
 *         <li>  Tracking basic counters for connections - active / pending / blocked / idle / busy / closed etc </li>
 *         <li>  Tracking latency scores for connections to this host.  </li>
 *         <li>  Tracking failures for connections to this host. </li>
 *         </ol> 
 *     </ol>
 *     
 * This class is intended to be used within a collection of {@link HostConnectionPool} tracked by a
 * {@link ConnectionPool} for all the {@link Host}(s) within a cassandra cluster. 
 * 
 * @see {@link SimpleHostConnectionPool} for sample implementations of this class. 
 * @see {@link ConnectionPool} for references to this class. 
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
     * @return A borrowed connection.  Connection must be returned either by calling returnConnection 
     *  or closeConnection.
     * @throws ConnectionException
     */
    Connection<CL> borrowConnection(int timeout) throws ConnectionException;

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
    int primeConnections(int numConnections) throws ConnectionException, InterruptedException;

    /**
     * @return Get the host to which this pool is associated
     */
    Host getHost();

    /**
     * @return Get number of open connections including any that are currently borrowed
     * and those that are currently idel
     */
    int getActiveConnectionCount();

    /**
     * @return Get the number of pending connection open attempts
     */
    int getPendingConnectionCount();

    /**
     * @return Get number of threads blocked waiting for a free connection
     */
    int getBlockedThreadCount();

    /**
     * @return Return the number of idle active connections. These are connections that
     * can be borrowed immediatley without having to make a new connection to
     * the remote server.
     */
    int getIdleConnectionCount();

    /**
     * @return Get number of currently borrowed connections
     */
    int getBusyConnectionCount();

    /**
     * @return Return true if the pool is marked as down and is trying to reconnect
     */
    boolean isReconnecting();

    /**
     * @return Return true if the pool is active.
     */
    boolean isActive();
    
    /**
     * @return Return true if the has been shut down and is no longer accepting traffic.
     */
    boolean isShutdown();
    
    /**
     * @return Return implementation specific score to be used by weighted pool
     * selection algorithms
     */
    double getScore();

    /**
     * Add a single latency sample after an operation on a connection belonging
     * to this pool
     * 
     * @param lastLatency
     */
    void addLatencySample(long lastLatency, long now);

    /**
     * @return Get total number of connections opened since the pool was created
     */
    int getOpenedConnectionCount();

    /**
     * @return Get the total number of failed connection open attempts
     */
    int getFailedOpenConnectionCount();

    /**
     * @return Get total number of connections closed
     */
    int getClosedConnectionCount();

    /**
     * @return Get number of errors since the last successful operation
     */
    int getErrorsSinceLastSuccess();

    /**
     * @return Return the number of open connection attempts
     */
    int getConnectAttemptCount();

}

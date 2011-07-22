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
import com.netflix.astyanax.connectionpool.exceptions.OperationException;

/**
 * Pool of connections for a single host
 * @author elandau
 *
 * @param <CL>
 */
public interface HostConnectionPool<CL> {
	/**
	 * Borrow a connection from the host.  May create a new connection if one
	 * is not available.
	 * @param timeout
	 * @return
	 * @throws ConnectionException
	 * @throws OperationException 
	 */
	Connection<CL> borrowConnection(int timeout) throws ConnectionException, OperationException;

	/**
	 * Return a connection to the host's pool
	 * @param connection
	 */
	void returnConnection(Connection<CL> connection);

	/**
	 * Get number of open connections including any that are currently borrowed
	 * and those that are currently idel
	 * @return
	 */
	int getActiveConnectionCount();

	/**
	 * Shut down the host so no more connections may be created when borrowConnections
	 * is called and connections will be terminated when returnConnection
	 * is called.
	 */
	void shutdown();
	
	/**
	 * Get the host to which this pool is associated
	 * @return
	 */
	Host getHost();

	/**
	 * Return the number of idle active connections.  These are connections
	 * that can be borrowed immediatley without having to make a new connection
	 * to the remote server.
	 * @return
	 */
	int getIdleConnectionCount();

}

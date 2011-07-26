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
package com.netflix.astyanax.mock;

import java.util.List;
import java.util.Map;

import com.netflix.astyanax.connectionpool.Connection;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ExecuteWithFailover;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;

public class MockConnectionPool implements ConnectionPool<MockClient> {

	Map<String, List<Host>> ring;
	
	public Map<String, List<Host>> getHosts() {
		return this.ring;
	}
	
	@Override
	public <R> Connection<MockClient> borrowConnection(
			Operation<MockClient, R> op) throws ConnectionException,
			OperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void returnConnection(Connection<MockClient> connection) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addHost(Host host) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeHost(Host host) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setHosts(Map<String, List<Host>> ring) {
		this.ring = ring;
	}

	@Override
	public <R> OperationResult<R> executeWithFailover(
			Operation<MockClient, R> op) throws ConnectionException,
			OperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void shutdown() {
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <R> ExecuteWithFailover<MockClient, R> newExecuteWithFailover()
			throws ConnectionException {
		// TODO Auto-generated method stub
		return null;
	}

}

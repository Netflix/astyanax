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
package com.netflix.astyanax.thrift;

import java.util.Map;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;

import com.google.common.collect.Maps;
import com.netflix.astyanax.AuthenticationStrategy;

public class BasicThriftAuthenticationStrategyImpl implements AuthenticationStrategy<Client> {

	private final String username;
	private final String password;
	
	public BasicThriftAuthenticationStrategyImpl(final String username, final String password) {
		this.username = username;
		this.password = password;
	}
	
	@Override
	public void authenticate(final Client thriftClient) throws com.netflix.astyanax.connectionpool.exceptions.AuthenticationException  {
		
		final Map<String,String> credentials = Maps.newHashMapWithExpectedSize(2);
		credentials.put("username", username);
		credentials.put("password", password);
		
		try {
			
			thriftClient.login(new AuthenticationRequest(credentials));
			
		} catch (AuthenticationException e) {
			throw new com.netflix.astyanax.connectionpool.exceptions.AuthenticationException(e.getMessage(), e.getCause());
		} catch (AuthorizationException e) {
			throw new com.netflix.astyanax.connectionpool.exceptions.AuthenticationException(e.getMessage(), e.getCause());
		} catch (TException e) {
			throw new com.netflix.astyanax.connectionpool.exceptions.AuthenticationException(e.getMessage(), e.getCause());
		}
		
	}

}

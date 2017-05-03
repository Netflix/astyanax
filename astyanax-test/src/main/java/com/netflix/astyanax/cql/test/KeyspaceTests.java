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
package com.netflix.astyanax.cql.test;

import org.apache.log4j.PropertyConfigurator;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.cql.test.utils.AstyanaxContextFactory;

public class KeyspaceTests {
	
	public static AstyanaxContext<Keyspace> context;
	public static Keyspace keyspace;
    
	public KeyspaceTests() {
	}
	
    public static void initContext() throws Exception {
    	PropertyConfigurator.configure("./src/main/resources/test-log4j.properties");
    	keyspace = AstyanaxContextFactory.getCachedKeyspace();
    }
}

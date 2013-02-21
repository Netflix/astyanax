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

import org.joda.time.DateTime;

import com.netflix.astyanax.connectionpool.NodeDiscovery;

public class EmptyNodeDiscoveryImpl implements NodeDiscovery {

    private static final EmptyNodeDiscoveryImpl instance = new EmptyNodeDiscoveryImpl();

    private EmptyNodeDiscoveryImpl() {

    }

    public static EmptyNodeDiscoveryImpl get() {
        return instance;
    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public DateTime getLastRefreshTime() {
        return null;
    }

    @Override
    public long getRefreshCount() {
        return 0;
    }

    @Override
    public long getErrorCount() {
        return 0;
    }

    @Override
    public Exception getLastException() {
        return null;
    }

    @Override
    public String getRawHostList() {
        return "";
    }

    public String toString() {
        return "EmptyNodeDiscovery";
    }

}

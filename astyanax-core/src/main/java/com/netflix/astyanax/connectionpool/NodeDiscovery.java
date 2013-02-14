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

import org.joda.time.DateTime;

/**
 * Interface for a module that periodically updates the nodes in a connection
 * pool.
 * 
 * @author elandau
 * 
 */
public interface NodeDiscovery {
    /**
     * Start the node discovery thread
     */
    void start();

    /**
     * Stop the node discovery thread
     */
    void shutdown();

    /**
     * @return Get the last refresh time in the discovery thread
     */
    DateTime getLastRefreshTime();

    /**
     * @return Get the number of refreshes
     */
    long getRefreshCount();

    /**
     * @return Get total number of errors encountered during a refresh
     */
    long getErrorCount();

    /**
     * @return Get the last exception that was encountered
     */
    Exception getLastException();

    /**
     * @return Get the raw list of nodes from the underlying refresh mechanism
     */
    String getRawHostList();
}

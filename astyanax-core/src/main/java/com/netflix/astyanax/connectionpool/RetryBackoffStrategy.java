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

/**
 * Strategy used to calculate how much to back off for each subsequent attempt
 * to reconnect to a downed host
 * 
 * @author elandau
 * 
 */
public interface RetryBackoffStrategy {
    public interface Callback {
        boolean tryConnect(int attemptCount);
    }

    public interface Instance {
        /**
         * Start the reconnect process
         */
        void begin();

        /**
         * Called when a connection was established successfully
         */
        void success();

        /**
         * @return Return the next backoff delay in the strategy
         */
        long getNextDelay();

        /**
         * Suspend the host for being bad (i.e. timing out too much)
         */
        void suspend();

        /**
         * @return Number of failed attempts
         */
        int getAttemptCount();
    };

    /**
     * Create an instance of the strategy for a single host
     */
    Instance createInstance();
}

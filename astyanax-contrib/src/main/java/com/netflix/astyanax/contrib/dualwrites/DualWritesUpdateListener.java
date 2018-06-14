/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.contrib.dualwrites;

/**
 * Notification based listener that gets update from some controller when changing the behavior of dual writes. 
 * This is what folks will need to implement to be able to react to the dual writes migration process. 
 * @see {@link DualWritesKeyspace}
 * 
 * @author poberai
 *
 */
public interface DualWritesUpdateListener {

    /**
     * Start dual writes
     */
    public void dualWritesEnabled();
    
    /**
     * Stop dual writes
     */
    public void dualWritesDisabled();
    
    /**
     * Flip roles of primary and secondary keyspaces. 
     * 
     * @param newDualKeyspaceSetup
     */
    public void flipPrimaryAndSecondary();
}

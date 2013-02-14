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
package com.netflix.astyanax.recipes.locks;

/**
 * Base interface to acquiring and release a row lock
 * 
 * Usage:
 * 
 * DistributedRowLock lock = new SomeLockImplementation(...); try {
 * lock.acquire(); // Do something ... } catch (BusyLockException) { // The lock
 * was already taken by a different process } catch (StaleLockException) { //
 * The row has a stale lock that needs to be addressed // This is usually caused
 * when no column TTL is set and the client // crashed before releasing the
 * lock. The DistributedRowLock should // have the option to auto delete stale
 * locks. } finally { lock.release(); }
 * 
 * @author elandau
 * 
 */
public interface DistributedRowLock {
    void acquire() throws BusyLockException, StaleLockException, Exception;

    void release() throws Exception;
}

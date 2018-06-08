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
package com.netflix.astyanax.connectionpool;

/**
 * Very very simple interface for a rate limiter. The basic idea is that clients
 * will call check() to determine if an operation may be performed. The concrete
 * rate limiter will update its internal state for each call to check
 * 
 * @author elandau
 */
public interface RateLimiter {
    boolean check();

    boolean check(long currentTimeMillis);
}

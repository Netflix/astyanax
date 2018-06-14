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
package com.netflix.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Rows;

public interface RowCallback<K, C> {
    /**
     * Notification for each block of rows.
     * 
     * @param rows
     */
    void success(Rows<K, C> rows);

    /**
     * Notification of an error calling cassandra. In your handler you can
     * implement your own backoff logic and return true to retry or false to
     * stop the query.
     * 
     * @param e
     * @return true to retry or false to exit
     */
    boolean failure(ConnectionException e);
}

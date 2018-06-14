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

import com.netflix.astyanax.model.ColumnFamily;

/**
 * TODO: Rename to AstyanaxTracerFactory
 * 
 * @author elandau
 * 
 */
public interface KeyspaceTracerFactory {
    /**
     * Create a tracer for cluster level operations
     * 
     * @param type
     * @return
     */
    CassandraOperationTracer newTracer(CassandraOperationType type);

    /**
     * Create a tracer for a column family operation
     * 
     * @param type
     * @param columnFamily
     * @return
     */
    CassandraOperationTracer newTracer(CassandraOperationType type, ColumnFamily<?, ?> columnFamily);
}

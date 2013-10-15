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
package com.netflix.astyanax.recipes.storage;

import java.nio.ByteBuffer;

import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;

public interface ChunkedStorageProvider {
    /**
     * Write a single chunk to the storage
     * 
     * @param chunk
     * @returns bytes written
     * @throws Exception
     */
    int writeChunk(String objectName, int chunkId, ByteBuffer data, Integer ttl) throws Exception;

    /**
     * Read the request chunk id from the storage
     * 
     * @param name
     * @param chunkId
     */
    ByteBuffer readChunk(String objectName, int chunkId) throws Exception, NotFoundException;

    /**
     * Delete a chunk
     * 
     * @param objectName
     * @param chunkId
     * @throws Exception
     */
    void deleteObject(String objectName, Integer chunkCount) throws Exception;

    /**
     * Persist all attributes for an object. Some attributes are written at the
     * start of the operation but are updated after the file has been written
     * with additional information such as the total number of chunks and the
     * file size
     * 
     * @param objectName
     * @param attr
     * @throws Exception
     */
    void writeMetadata(String objectName, ObjectMetadata attr) throws Exception;

    /**
     * Retrieve information for a file
     * 
     * @param objectName
     * @throws Exception
     */
    ObjectMetadata readMetadata(String objectName) throws Exception, NotFoundException;

    /**
     * @return Return the preferred chunk size for this provider
     */
    int getDefaultChunkSize();
}

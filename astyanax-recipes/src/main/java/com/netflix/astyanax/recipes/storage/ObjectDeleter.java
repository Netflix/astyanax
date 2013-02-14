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

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectDeleter implements Callable<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(ObjectDeleter.class);

    private final ChunkedStorageProvider provider;
    private final String objectName;
    private Integer chunkCount = null; // This will default to all being deleted

    public ObjectDeleter(ChunkedStorageProvider provider, String objectName) {
        this.provider = provider;
        this.objectName = objectName;
    }

    public ObjectDeleter withChunkCountToDelete(int count) {
        this.chunkCount = count;
        return this;
    }

    @Override
    public Void call() throws Exception {
        LOG.info("Deleting " + objectName);
        provider.deleteObject(objectName, chunkCount);
        return null;
    }
}

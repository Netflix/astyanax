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

import java.io.InputStream;
import java.io.OutputStream;

public class ChunkedStorage {
    public static ObjectWriter newWriter(ChunkedStorageProvider provider, String objectName, InputStream is) {
        return new ObjectWriter(provider, objectName, is);
    }

    public static ObjectReader newReader(ChunkedStorageProvider provider, String objectName, OutputStream os) {
        return new ObjectReader(provider, objectName, os);
    }

    public static ObjectDeleter newDeleter(ChunkedStorageProvider provider, String objectName) {
        return new ObjectDeleter(provider, objectName);
    }

    public static ObjectInfoReader newInfoReader(ChunkedStorageProvider provider, String objectName) {
        return new ObjectInfoReader(provider, objectName);
    }
    
    public static ObjectDirectoryLister newObjectDirectoryLister(ChunkedStorageProvider provider, String path) {
        return new ObjectDirectoryLister(provider, path);
    }
}

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

public class ObjectMetadata {
    private Integer ttl;
    private Long objectSize;
    private Integer chunkCount;
    private Integer chunkSize;
    private String parentPath;
    private String attributes;

    public ObjectMetadata setTtl(Integer ttl) {
        this.ttl = ttl;
        return this;
    }

    public Integer getTtl() {
        return this.ttl;
    }

    public boolean hasTtl() {
        return this.ttl != null && this.ttl > 0;
    }

    public Long getObjectSize() {
        return objectSize;
    }

    public ObjectMetadata setObjectSize(Long objectSize) {
        this.objectSize = objectSize;
        return this;
    }

    public Integer getChunkCount() {
        return chunkCount;
    }

    public ObjectMetadata setChunkCount(Integer chunkCount) {
        this.chunkCount = chunkCount;
        return this;
    }

    public Integer getChunkSize() {
        return chunkSize;
    }

    public ObjectMetadata setChunkSize(Integer chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public boolean isValidForRead() {
        return (this.objectSize != null && this.chunkCount != null && this.chunkSize != null);
    }
    
    public ObjectMetadata setParentPath(String parentPath) {
        this.parentPath = parentPath;
        return this;
    }
    
    public String getParentPath() {
        return this.parentPath;
    }
    
    public ObjectMetadata setAttributes(String attributes) {
        this.attributes = attributes;
        return this;
    }
    
    public String getAttributes() {
        return this.attributes;
    }
}

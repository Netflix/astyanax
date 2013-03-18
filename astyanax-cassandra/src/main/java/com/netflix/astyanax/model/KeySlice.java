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
package com.netflix.astyanax.model;

import java.util.Collection;

/**
 * Definition of a set of keys. The set can be either a fixed set of keys, a
 * range of keys or a range of tokens.
 * 
 * @author elandau
 * 
 * @param <K>
 */
public class KeySlice<K> {
    private Collection<K> keys;
    private K startKey;
    private K endKey;
    private String startToken;
    private String endToken;
    private int size = 0;

    public KeySlice(Collection<K> keys) {
        this.keys = keys;
    }

    public KeySlice(K startKey, K endKey, String startToken, String endToken, int size) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.startToken = startToken;
        this.endToken = endToken;
        this.size = size;
    }

    public Collection<K> getKeys() {
        return keys;
    }

    public K getStartKey() {
        return startKey;
    }

    public K getEndKey() {
        return endKey;
    }

    public String getStartToken() {
        return startToken;
    }

    public String getEndToken() {
        return endToken;
    }

    public int getLimit() {
        return size;
    }
}

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
package com.netflix.astyanax.thrift.model;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;

public class ThriftRowImpl<K, C> implements Row<K, C> {
    private final ColumnList<C> columns;
    private final ByteBuffer rawkey;
    private final K key;

    public ThriftRowImpl(K key, ByteBuffer byteBuffer, ColumnList<C> columns) {
        this.key = key;
        this.columns = columns;
        this.rawkey = byteBuffer;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public ColumnList<C> getColumns() {
        return columns;
    }

    @Override
    public ByteBuffer getRawKey() {
        return this.rawkey;
    }
}

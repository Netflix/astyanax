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

import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Rows;

public class ThriftCqlResultImpl<K, C> implements CqlResult<K, C> {
    private final Rows<K, C> rows;
    private final Integer number;

    public ThriftCqlResultImpl(Rows<K, C> rows) {
        this.rows = rows;
        this.number = null;
    }

    public ThriftCqlResultImpl(Integer count) {
        this.rows = null;
        this.number = count;
    }

    @Override
    public Rows<K, C> getRows() {
        return rows;
    }

    @Override
    public int getNumber() {
        return number;
    }

    @Override
    public boolean hasRows() {
        return rows != null && !this.rows.isEmpty();
    }

    @Override
    public boolean hasNumber() {
        return number != null;
    }

}

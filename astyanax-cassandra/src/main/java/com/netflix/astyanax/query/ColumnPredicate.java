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
package com.netflix.astyanax.query;

import com.netflix.astyanax.model.Equality;

public class ColumnPredicate {
    private String   name;
    private Equality op;
    private Object   value;
    
    public String getName() {
        return name;
    }

    public Equality getOp() {
        return op;
    }

    public Object getValue() {
        return value;
    }

    public ColumnPredicate setName(String name) {
        this.name = name;
        return this;
    }

    public ColumnPredicate setOp(Equality op) {
        this.op = op;
        return this;
    }

    public ColumnPredicate setValue(Object value) {
        this.value = value;
        return this;
    }
    
    @Override
    public String toString() {
        return "ColumnPredicate [name=" + name + ", op=" + op + ", value="
                + value + "]";
    }

    
}

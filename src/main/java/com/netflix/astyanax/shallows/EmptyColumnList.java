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
package com.netflix.astyanax.shallows;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.AbstractColumnList;
import com.netflix.astyanax.model.Column;

public class EmptyColumnList<C> extends AbstractColumnList<C> {

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Column<C>> iterator() {
        return new EmptyIterator();
    }

    @Override
    public Column<C> getColumnByName(C columnName) {
        return null;
    }

    @Override
    public Column<C> getColumnByIndex(int idx) {
        return null;
    }

    @Override
    public <C2> Column<C2> getSuperColumn(C columnName, Serializer<C2> colSer) {
        return null;
    }

    @Override
    public <C2> Column<C2> getSuperColumn(int idx, Serializer<C2> colSer) {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isSuperColumn() {
        return false;
    }

    @Override
    public Collection<C> getColumnNames() {
        return new HashSet<C>();
    }
}

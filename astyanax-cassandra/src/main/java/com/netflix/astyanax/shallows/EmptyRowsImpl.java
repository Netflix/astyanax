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
import java.util.Iterator;

import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class EmptyRowsImpl<K, C> implements Rows<K, C> {

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row<K, C>> iterator() {
        return new EmptyIterator();
    }

    @Override
    public Row<K, C> getRow(K key) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public Row<K, C> getRowByIndex(int i) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<K> getKeys() {
        // TODO Auto-generated method stub
        return null;
    }
}

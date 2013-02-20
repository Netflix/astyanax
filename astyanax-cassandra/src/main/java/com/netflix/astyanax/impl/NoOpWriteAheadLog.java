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
package com.netflix.astyanax.impl;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.WriteAheadEntry;
import com.netflix.astyanax.WriteAheadLog;
import com.netflix.astyanax.connectionpool.exceptions.WalException;

public class NoOpWriteAheadLog implements WriteAheadLog {

    @Override
    public WriteAheadEntry createEntry() throws WalException {
        return new WriteAheadEntry() {
            @Override
            public void readMutation(MutationBatch mutation) throws WalException {
            }

            @Override
            public void writeMutation(MutationBatch mutation) throws WalException {
            }
        };
    }

    @Override
    public void removeEntry(WriteAheadEntry entry) {
    }

    @Override
    public WriteAheadEntry readNextEntry() {
        return null;
    }

    @Override
    public void retryEntry(WriteAheadEntry entry) {
    }

}

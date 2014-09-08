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

public enum Equality {
    LESS_THAN((byte) -1), GREATER_THAN_EQUALS((byte) -1), EQUAL((byte) 0), GREATER_THAN((byte) 1), LESS_THAN_EQUALS(
            (byte) 1);

    private final byte equality;

    Equality(byte equality) {
        this.equality = equality;
    }

    public byte toByte() {
        return equality;
    }

    public static Equality fromByte(byte equality) {
        if (equality > 0) {
            return GREATER_THAN;
        }
        if (equality < 0) {
            return LESS_THAN;
        }
        return EQUAL;
    }
}

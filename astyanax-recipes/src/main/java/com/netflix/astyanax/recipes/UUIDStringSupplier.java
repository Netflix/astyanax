/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.recipes;

import java.util.UUID;

import com.google.common.base.Supplier;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class UUIDStringSupplier implements Supplier<String> {
    private static final UUIDStringSupplier instance = new UUIDStringSupplier();

    public static UUIDStringSupplier getInstance() {
        return instance;
    }

    @Override
    public String get() {
        UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMicros();
        return uuid.toString();
    }
}

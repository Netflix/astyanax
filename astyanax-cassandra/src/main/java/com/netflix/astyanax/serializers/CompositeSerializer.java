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
package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.List;

import com.netflix.astyanax.model.Composite;

public class CompositeSerializer extends AbstractSerializer<Composite> {

    private static final CompositeSerializer instance = new CompositeSerializer();

    public static CompositeSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Composite obj) {
        return obj.serialize();
    }

    @Override
    public Composite fromByteBuffer(ByteBuffer byteBuffer) {
        Composite composite = new Composite();
        composite.setComparatorsByPosition(getComparators());
        composite.deserialize(byteBuffer);
        return composite;
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        throw new IllegalStateException(
                "Composite columns can't be paginated this way.  Use SpecificCompositeSerializer instead.");
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.COMPOSITETYPE;
    }

    public List<String> getComparators() {
        return null;
    }
}

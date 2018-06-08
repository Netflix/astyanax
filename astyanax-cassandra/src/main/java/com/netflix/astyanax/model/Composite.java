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
package com.netflix.astyanax.model;

import java.nio.ByteBuffer;
import java.util.List;

public class Composite extends AbstractComposite {

    public Composite() {
        super(false);
    }

    public Composite(Object... o) {
        super(false, o);
    }

    public Composite(List<?> l) {
        super(false, l);
    }

    public static Composite fromByteBuffer(ByteBuffer byteBuffer) {

        Composite composite = new Composite();
        composite.deserialize(byteBuffer);

        return composite;
    }

    public static ByteBuffer toByteBuffer(Object... o) {
        Composite composite = new Composite(o);
        return composite.serialize();
    }

    public static ByteBuffer toByteBuffer(List<?> l) {
        Composite composite = new Composite(l);
        return composite.serialize();
    }
}

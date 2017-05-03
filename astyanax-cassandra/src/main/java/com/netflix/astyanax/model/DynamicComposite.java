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

import java.nio.ByteBuffer;
import java.util.List;

public class DynamicComposite extends AbstractComposite {

    public DynamicComposite() {
        super(true);
    }

    public DynamicComposite(Object... o) {
        super(true, o);
    }

    public DynamicComposite(List<?> l) {
        super(true, l);
    }

    public static DynamicComposite fromByteBuffer(ByteBuffer byteBuffer) {
        DynamicComposite composite = new DynamicComposite();
        composite.deserialize(byteBuffer);

        return composite;
    }

    public static ByteBuffer toByteBuffer(Object... o) {
        DynamicComposite composite = new DynamicComposite(o);
        return composite.serialize();
    }

    public static ByteBuffer toByteBuffer(List<?> l) {
        DynamicComposite composite = new DynamicComposite(l);
        return composite.serialize();
    }
}
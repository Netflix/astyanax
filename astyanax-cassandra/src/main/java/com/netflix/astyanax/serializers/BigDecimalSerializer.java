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
package com.netflix.astyanax.serializers;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.DecimalType;

public class BigDecimalSerializer extends AbstractSerializer<BigDecimal> {

    private static final BigDecimalSerializer INSTANCE = new BigDecimalSerializer();

    public static BigDecimalSerializer get() {
        return INSTANCE;
    }

    @Override
    public BigDecimal fromByteBuffer(final ByteBuffer byteBuffer) {
        if (byteBuffer == null)
            return null;
        return DecimalType.instance.compose(byteBuffer.duplicate());
    }

    @Override
    public ByteBuffer toByteBuffer(BigDecimal obj) {
        return DecimalType.instance.decompose(obj);
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.DECIMALTYPE;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return DecimalType.instance.fromString(str);
    }

    @Override
    public String getString(final ByteBuffer byteBuffer) {
        if (byteBuffer == null)
            return null;
        return DecimalType.instance.getString(byteBuffer.duplicate());
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        throw new RuntimeException("Not supported");
    }

}

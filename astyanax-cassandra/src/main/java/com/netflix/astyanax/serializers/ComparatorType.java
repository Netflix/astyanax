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

import com.netflix.astyanax.*;

/**
 * @author: peter
 */
public enum ComparatorType {

    ASCIITYPE("org.apache.cassandra.db.marshal.AsciiType", AsciiSerializer.get()), 
    BYTESTYPE("org.apache.cassandra.db.marshal.BytesType", ByteBufferSerializer.get()), 
    INTEGERTYPE("org.apache.cassandra.db.marshal.IntegerType", BigIntegerSerializer.get()), 
    INT32TYPE("org.apache.cassandra.db.marshal.Int32Type", Int32Serializer.get()), 
    DECIMALTYPE("org.apache.cassandra.db.marshal.DecimalType", BigDecimalSerializer.get()),
    LEXICALUUIDTYPE("org.apache.cassandra.db.marshal.LexicalUUIDType", UUIDSerializer.get()), 
    LOCALBYPARTITIONERTYPE("org.apache.cassandra.db.marshal.LocalByPartionerType", ByteBufferSerializer.get()), // FIXME
    LONGTYPE("org.apache.cassandra.db.marshal.LongType", LongSerializer.get()), 
    TIMEUUIDTYPE("org.apache.cassandra.db.marshal.TimeUUIDType", TimeUUIDSerializer.get()), 
    UTF8TYPE("org.apache.cassandra.db.marshal.UTF8Type", StringSerializer.get()), 
    COMPOSITETYPE("org.apache.cassandra.db.marshal.CompositeType", CompositeSerializer.get()), 
    DYNAMICCOMPOSITETYPE("org.apache.cassandra.db.marshal.DynamicCompositeType", DynamicCompositeSerializer.get()), 
    UUIDTYPE("org.apache.cassandra.db.marshal.UUIDType", UUIDSerializer.get()), 
    COUNTERTYPE("org.apache.cassandra.db.marshal.CounterColumnType", LongSerializer.get()), 
    DOUBLETYPE("org.apache.cassandra.db.marshal.DoubleType", DoubleSerializer.get()), 
    FLOATTYPE("org.apache.cassandra.db.marshal.FloatType", FloatSerializer.get()), 
    BOOLEANTYPE("org.apache.cassandra.db.marshal.BooleanType", BooleanSerializer.get()),
    DATETYPE("org.apache.cassandra.db.marshal.DateType", DateSerializer.get()),
    REVERSEDTYPE("org.apache.cassandra.db.marshal.ReversedType", ReversedSerializer.get());

    private final String className;
    private final String typeName;
    private final Serializer<?> serializer;

    private ComparatorType(String className, Serializer<?> serializer) {
        this.className = className;
        if (className.startsWith("org.apache.cassandra.db.marshal.")) {
            typeName = className.substring("org.apache.cassandra.db.marshal.".length());
        }
        else {
            typeName = className;
        }
        this.serializer = serializer;
    }

    public String getClassName() {
        return className;
    }

    public String getTypeName() {
        return typeName;
    }

    public Serializer<?> getSerializer() {
        return serializer;
    }

    public static ComparatorType getByClassName(String className) {
        if (className == null) {
            return null;
        }

        for (ComparatorType type : ComparatorType.values()) {
            if (type.getClassName().equals(className)) {
                return type;
            }
            if (type.getClassName().equals("org.apache.cassandra.db.marshal." + className)) {
                return type;
            }
        }
        return null;
    }
}

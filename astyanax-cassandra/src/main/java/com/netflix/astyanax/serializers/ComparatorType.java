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

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.shaded.org.apache.cassandra.db.marshal.ShadedTypeParser;

import static com.netflix.astyanax.shaded.org.apache.cassandra.db.marshal.ShadedTypeParser.SHADED_PREFIX;

/**
 * @author: peter
 */
public enum ComparatorType {

    ASCIITYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.AsciiType", AsciiSerializer.get()),
    BYTESTYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.BytesType", ByteBufferSerializer.get()),
    INTEGERTYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.IntegerType", BigIntegerSerializer.get()),
    INT32TYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.Int32Type", Int32Serializer.get()),
    DECIMALTYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.DecimalType", BigDecimalSerializer.get()),
    LEXICALUUIDTYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.LexicalUUIDType", UUIDSerializer.get()),
    LOCALBYPARTITIONERTYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.LocalByPartionerType", ByteBufferSerializer.get()), // FIXME
    LONGTYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.LongType", LongSerializer.get()),
    TIMEUUIDTYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.TimeUUIDType", TimeUUIDSerializer.get()),
    UTF8TYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.UTF8Type", StringSerializer.get()),
    COMPOSITETYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.CompositeType", CompositeSerializer.get()),
    DYNAMICCOMPOSITETYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.DynamicCompositeType", DynamicCompositeSerializer.get()),
    UUIDTYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.UUIDType", UUIDSerializer.get()),
    COUNTERTYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.CounterColumnType", LongSerializer.get()),
    DOUBLETYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.DoubleType", DoubleSerializer.get()),
    FLOATTYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.FloatType", FloatSerializer.get()),
    BOOLEANTYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.BooleanType", BooleanSerializer.get()),
    DATETYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.DateType", DateSerializer.get()),
    REVERSEDTYPE(SHADED_PREFIX + "org.apache.cassandra.db.marshal.ReversedType", ReversedSerializer.get());

    private final String className;
    private final String typeName;
    private final Serializer<?> serializer;

    private ComparatorType(String className, Serializer<?> serializer) {
        this.className = className;
        this.typeName = getShadedTypeName(className);
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
            if (type.getClassName().equals(getShadedClassName(className))) {
                return type;
            }
        }
        return null;
    }

    public static String getShadedClassName(String className){
        return ShadedTypeParser.getShadedClassName(className);
    }

    public static String getShadedTypeName(String typeName){
        return ShadedTypeParser.getShadedTypeName(typeName);
    }
}

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
package com.netflix.astyanax.test;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnType;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;

public class TestConstants {

    public static final ColumnFamily<String, String> CF_STANDARD1 = new ColumnFamily<String, String>(
            "Standard1", StringSerializer.get(), StringSerializer.get());

    public static final ColumnFamily<String, Long> CF_LONGCOLUMN = new ColumnFamily<String, Long>(
            "LongColumn1", StringSerializer.get(), LongSerializer.get());

    public static final ColumnFamily<String, String> CF_STANDARD2 = new ColumnFamily<String, String>(
            "Standard2", StringSerializer.get(), StringSerializer.get());

    public static final ColumnFamily<String, String> CF_SUPER1 = new ColumnFamily<String, String>(
            "Super1", StringSerializer.get(), StringSerializer.get(),
            ColumnType.SUPER);

    public static final ColumnFamily<String, String> CF_COUNTER1 = new ColumnFamily<String, String>(
            "Counter1", StringSerializer.get(), StringSerializer.get());

    public static final ColumnFamily<String, String> CF_COUNTER_SUPER1 = new ColumnFamily<String, String>(
            "CounterSuper1", StringSerializer.get(), StringSerializer.get(),
            ColumnType.SUPER);

    public static final ColumnFamily<String, String> CF_NOT_DEFINED = new ColumnFamily<String, String>(
            "NotDefined", StringSerializer.get(), StringSerializer.get());

    public static final ColumnFamily<String, String> CF_EMPTY = new ColumnFamily<String, String>(
            "NotDefined", StringSerializer.get(), StringSerializer.get());

    public static final ColumnFamily<String, TestCompositeType> CF_COMPOSITE = new ColumnFamily<String, TestCompositeType>(
            "CompositeColumn", StringSerializer.get(),
            new AnnotatedCompositeSerializer<TestCompositeType>(
                    TestCompositeType.class));

    public static final ColumnFamily<ByteBuffer, ByteBuffer> CF_COMPOSITE_CSV = new ColumnFamily<ByteBuffer, ByteBuffer>(
            "CompositeCsv", ByteBufferSerializer.get(),
            ByteBufferSerializer.get());

    public static final ColumnFamily<TestCompositeType, String> CF_COMPOSITE_KEY = new ColumnFamily<TestCompositeType, String>(
            "CompositeKey",
            new AnnotatedCompositeSerializer<TestCompositeType>(
                    TestCompositeType.class), StringSerializer.get());

    public static final ColumnFamily<String, UUID> CF_TIME_UUID = new ColumnFamily<String, UUID>(
            "TimeUUID1", StringSerializer.get(), TimeUUIDSerializer.get());

    public static final AnnotatedCompositeSerializer<SessionEvent> SE_SERIALIZER = new AnnotatedCompositeSerializer<SessionEvent>(
            SessionEvent.class);

    public static final ColumnFamily<String, SessionEvent> CF_CLICK_STREAM = new ColumnFamily<String, SessionEvent>(
            "ClickStream", StringSerializer.get(), SE_SERIALIZER);

    public static final String CLUSTER_NAME = "TestCluster1";
    public static final String KEYSPACE_NAME = "Keyspace1";

}

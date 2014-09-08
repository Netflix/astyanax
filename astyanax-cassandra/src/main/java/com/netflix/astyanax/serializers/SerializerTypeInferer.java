package com.netflix.astyanax.serializers;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.netflix.astyanax.Serializer;

/**
 * Utility class that infers the concrete Serializer needed to turn a value into
 * its binary representation
 * 
 * @author Bozhidar Bozhanov
 * 
 */
public class SerializerTypeInferer {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T> Serializer<T> getSerializer(Object value) {
        Serializer serializer = null;
        if (value == null) {
            serializer = ByteBufferSerializer.get();
        }
        else if (value instanceof UUID) {
            serializer = UUIDSerializer.get();
        }
        else if (value instanceof String) {
            serializer = StringSerializer.get();
        }
        else if (value instanceof Long) {
            serializer = LongSerializer.get();
        }
        else if (value instanceof Integer) {
            serializer = Int32Serializer.get();
        }
        else if (value instanceof Short) {
            serializer = ShortSerializer.get();
        }
        else if (value instanceof Byte) {
            serializer = ByteSerializer.get();
        }
        else if (value instanceof Float) {
            serializer = FloatSerializer.get();
        }
        else if (value instanceof Double) {
            serializer = DoubleSerializer.get();
        }
        else if (value instanceof BigInteger) {
            serializer = BigIntegerSerializer.get();
        }
        else if (value instanceof BigDecimal) {
            serializer = BigDecimalSerializer.get();
        }
        else if (value instanceof Boolean) {
            serializer = BooleanSerializer.get();
        }
        else if (value instanceof byte[]) {
            serializer = BytesArraySerializer.get();
        }
        else if (value instanceof ByteBuffer) {
            serializer = ByteBufferSerializer.get();
        }
        else if (value instanceof Date) {
            serializer = DateSerializer.get();
        }
		// TODO: need what to select StringCoercibleSerilizer here?
        else {
            serializer = ObjectSerializer.get();
        }
        // Add other serializers here

        return serializer;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T> Serializer<T> getSerializer(Class<?> valueClass) {
        Serializer serializer = null;
        if (valueClass.equals(UUID.class)) {
            serializer = UUIDSerializer.get();
        }
        else if (valueClass.equals(String.class)) {
            serializer = StringSerializer.get();
        }
        else if (valueClass.equals(Long.class) || valueClass.equals(long.class)) {
            serializer = LongSerializer.get();
        }
        else if (valueClass.equals(Integer.class) || valueClass.equals(int.class)) {
            serializer = Int32Serializer.get();
        }
        else if (valueClass.equals(Short.class) || valueClass.equals(short.class)) {
            serializer = ShortSerializer.get();
        }
        else if (valueClass.equals(Byte.class) || valueClass.equals(byte.class)) {
            serializer = ByteSerializer.get();
        }
        else if (valueClass.equals(Float.class) || valueClass.equals(float.class)) {
            serializer = FloatSerializer.get();
        }
        else if (valueClass.equals(Double.class) || valueClass.equals(double.class)) {
            serializer = DoubleSerializer.get();
        }
        else if (valueClass.equals(BigInteger.class)) {
            serializer = BigIntegerSerializer.get();
        }
        else if (valueClass.equals(BigDecimal.class)) {
            serializer = BigDecimalSerializer.get();
        }
        else if (valueClass.equals(Boolean.class) || valueClass.equals(boolean.class)) {
            serializer = BooleanSerializer.get();
        }
        else if (valueClass.equals(byte[].class)) {
            serializer = BytesArraySerializer.get();
        }
        else if (valueClass.equals(ByteBuffer.class)) {
            serializer = ByteBufferSerializer.get();
        }
        else if (valueClass.equals(Date.class)) {
            serializer = DateSerializer.get();
        }
		if (serializer == null) {
           	serializer = ObjectSerializer.get();
		}
        // Add other serializers here
        return serializer;
    }
}

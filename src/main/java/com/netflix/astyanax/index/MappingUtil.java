package com.netflix.astyanax.index;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.ObjectSerializer;
import com.netflix.astyanax.serializers.ShortSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class MappingUtil {

	
	static byte [] zeroByte = new byte[1];
	static byte [] byteBuffByte = new byte[1]; 
	static byte [] strByte = new byte[1];
	static byte [] byteArrByte = new byte[1];
	static byte [] longByte = new byte[1];
	static byte [] intByte = new byte[1];
	static byte [] shortByte = new byte[1];
	static byte [] compByte = new byte[1];
	static byte [] objByte = new byte[1];
	static byte [] bigIntByte = new byte[1];
	
	static Map<Class<?>,byte[]> clToByteMap = new HashMap<Class<?>,byte[]>();
	static Map<Class<?>,byte[]> byteToClMap = new HashMap<Class<?>,byte[]>();
	
	static {
		zeroByte[0] = new Integer(0).byteValue();
		clToByteMap.put(Class.class, zeroByte);
		
		strByte[0] = new Integer(1).byteValue();
		clToByteMap.put(String.class, strByte);
		
		byteArrByte[0] = new Integer(2).byteValue();
		clToByteMap.put(byte[].class, byteArrByte);
		
		longByte[0] = new Integer(3).byteValue();
		clToByteMap.put(Long.class, longByte);
		clToByteMap.put(long.class, longByte);
		
		intByte[0] = new Integer(4).byteValue(); 
		clToByteMap.put(Integer.class, intByte);
		clToByteMap.put(int.class, intByte);
		
		shortByte[0] = new Integer(5).byteValue();
		clToByteMap.put(Short.class, shortByte);
		clToByteMap.put(short.class, shortByte);
		
		
		byteBuffByte[0] = new Integer(6).byteValue();
		clToByteMap.put(ByteBuffer.class, byteBuffByte);
			
		
		objByte[0] = new Integer(7).byteValue();
		clToByteMap.put(Object.class, intByte);
				
		
		bigIntByte[0] = new Integer(8).byteValue();
		clToByteMap.put(BigInteger.class, bigIntByte);
		
		//byteArrByte[0] = new Integer(2).byteValue();
		
		
	}
	
	/**
	 * TODO: fix this to be a hashmap lookup.
	 * 
	 * @param b
	 * @return
	 */
	public static <K>Serializer<K> getSerializer(byte []allb) {
		Serializer<?> serializer = BytesArraySerializer.get();
		byte b = allb[0];
		
		if (strByte[0] == b) {
            serializer = StringSerializer.get();
        }
        else if (longByte[0] == b) {
            serializer = LongSerializer.get();
        }
        else if (intByte[0] == b) {
            serializer = IntegerSerializer.get();
        }
        else if (shortByte[0] == b) {
            serializer = ShortSerializer.get();
        }
       /* else if (valueClass.equals(Boolean.class) || valueClass.equals(boolean.class)) {
            serializer = BooleanSerializer.get();
        }*/
        else if (byteArrByte[0] == b) {
            serializer = BytesArraySerializer.get();
        }
        else if (byteBuffByte[0] == b) {
            serializer = ByteBufferSerializer.get();
        }
        else if (objByte[0] == b) {
            serializer = ObjectSerializer.get();
        }
		
		return (Serializer<K>)serializer;
	}
	
	public static byte[] zeroByte() {
		return zeroByte;
	}
	
	public static byte[] getType(Class<?> cl) {
		return clToByteMap.get(cl);
		
	}
	
	public static byte[] getType(Object value) {
		 
		byte [] ret = zeroByte;
	    ret = getType(value.getClass());   
	    return ret;
	}
	
	
	
}

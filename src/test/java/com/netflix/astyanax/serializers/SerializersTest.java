package com.netflix.astyanax.serializers;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.model.Composite;

public class SerializersTest {
	private static Logger LOG = LoggerFactory.getLogger(SerializersTest.class);

	private static BytesArraySerializer hexSerializer = new BytesArraySerializer();

	@Test
	public void testAsciiSerializer() {
		AsciiSerializer ser = new AsciiSerializer();
		String value = "Test";
		ByteBuffer byteBuffer = ser.fromString(value);
		Assert.assertEquals("54657374", hexSerializer.getString(byteBuffer));
		Assert.assertEquals(value, ser.getString(byteBuffer));
	}

	@Test
	public void testBigIntegerSerializer() {
		BigIntegerSerializer ser = new BigIntegerSerializer();

		BigInteger bi1 = new BigInteger("127");
		ByteBuffer bb1 = ser.toByteBuffer(bi1);
		BigInteger bi1_verify = ser.fromByteBuffer(bb1);
		ByteBuffer bb1_str = ser.fromString("127");
		ByteBuffer bb2 = ser.getNext(bb1);
		BigInteger bi2 = ser.fromByteBuffer(bb2);

		Assert.assertEquals(bi1, bi1_verify);
		Assert.assertEquals(bb1, bb1_str);
		Assert.assertEquals(1, bi2.intValue() - bi1.intValue());
		Assert.assertEquals(bb2.capacity(), bb1.capacity() + 1);
	}

	@Test
	public void testBooleanSerializer() {
		BooleanSerializer ser = new BooleanSerializer();
	}

	@Test
	public void testByteBufferSerializer() {
		ByteBufferSerializer ser = new ByteBufferSerializer();
	}

	@Test
	public void testCompressedSerializer() {
	    GzipStringSerializer ser = new GzipStringSerializer();
	    
	    String str = StringUtils.repeat("{The : 'quick', brown : 'fox', jumped : 'over', the : 'lazy cow'}", 100);

	    int origLength = str.length();
	    
	    ByteBuffer compressed = ser.toByteBuffer(str);
	    int compLength = compressed.limit();

	    String str2 = ser.fromByteBuffer(compressed);
	    Assert.assertEquals(str,  str2);
	    LOG.info(String.format("Compressed at ratio: %2f", (double)(origLength - compLength) / (double)origLength));
	    
	}
	
	@Test
	public void testBytesArraySerializer() {
		BytesArraySerializer ser = new BytesArraySerializer();
	}

	@Test
	public void testCharSerializer() {
		CharSerializer ser = new CharSerializer();
	}

	@Test
	public void testDataSerializer() {
		DateSerializer ser = new DateSerializer();
	}

	@Test
	public void testDoubleSerializer() {
		DoubleSerializer ser = new DoubleSerializer();

		Double d1 = 127.0;
		ByteBuffer bb1 = ser.toByteBuffer(d1);
		Double d1_verify = ser.fromByteBuffer(bb1.duplicate());
		ByteBuffer bb1_str = ser.fromString("127");
		ByteBuffer bb2 = ser.getNext(bb1);
		Double d2 = ser.fromByteBuffer(bb2.duplicate());

		Assert.assertEquals(d1, d1_verify);
		Assert.assertEquals(bb1, bb1_str);
		Assert.assertEquals(d1 + Double.MIN_VALUE, d2);
		Assert.assertEquals(bb2.capacity(), bb1.capacity());

		ByteBuffer bbMax = ser.toByteBuffer(Double.MAX_VALUE);
		try {
			ser.getNext(bbMax);
			Assert.fail();
		} catch (Exception e) {
			LOG.info(e.getMessage());
		}
	}

	@Test
	public void testFloatSerializer() {
		FloatSerializer ser = new FloatSerializer();

		Float f1 = (float) 127.0;
		ByteBuffer bb1 = ser.toByteBuffer(f1);
		Float f1_verify = ser.fromByteBuffer(bb1.duplicate());
		ByteBuffer bb1_str = ser.fromString("127");
		ByteBuffer bb2 = ser.getNext(bb1);
		Float f2 = ser.fromByteBuffer(bb2.duplicate());

		Assert.assertEquals(f1, f1_verify);
		Assert.assertEquals(bb1, bb1_str);
		Assert.assertEquals(f1 + Float.MIN_VALUE, f2);
		Assert.assertEquals(bb2.capacity(), bb1.capacity());

		ByteBuffer bbMax = ser.toByteBuffer(Float.MAX_VALUE);
		try {
			ser.getNext(bbMax);
			Assert.fail();
		} catch (Exception e) {
			LOG.info(e.getMessage());
		}
	}

	@Test
	public void testIntegerSerializer() {
		IntegerSerializer ser = new IntegerSerializer();

		Integer bi1 = 127;
		ByteBuffer bb1 = ser.toByteBuffer(bi1);
		Integer bi1_verify = ser.fromByteBuffer(bb1);
		ByteBuffer bb1_str = ser.fromString("127");
		ByteBuffer bb2 = ser.getNext(bb1);
		Integer bi2 = ser.fromByteBuffer(bb2);

		Assert.assertEquals(bi1, bi1_verify);
		Assert.assertEquals(bb1, bb1_str);
		Assert.assertEquals(1, bi2.intValue() - bi1.intValue());
		Assert.assertEquals(bb2.capacity(), bb1.capacity());

		ByteBuffer bbMax = ser.toByteBuffer(Integer.MAX_VALUE);
		try {
			ser.getNext(bbMax);
			Assert.fail();
		} catch (Exception e) {
			LOG.info(e.getMessage());
		}
	}

	@Test
	public void testLongSerializer() {
		LongSerializer ser = new LongSerializer();

		Long val1 = (long) 127;
		ByteBuffer bb1 = ser.toByteBuffer(val1);
		Long val1_verify = ser.fromByteBuffer(bb1.duplicate());
		ByteBuffer bb1_str = ser.fromString("127");
		ByteBuffer bb2 = ser.getNext(bb1);
		Long val2 = ser.fromByteBuffer(bb2.duplicate());

		Assert.assertEquals(val1, val1_verify);
		Assert.assertEquals(bb1, bb1_str);
		Assert.assertEquals(1, val2.intValue() - val1.intValue());
		Assert.assertEquals(bb2.capacity(), bb1.capacity());

		ByteBuffer bbMax = ser.toByteBuffer(Long.MAX_VALUE);
		try {
			ser.getNext(bbMax);
			Assert.fail();
		} catch (Exception e) {
			LOG.info(e.getMessage());
		}
	}

	@Test
	public void testByteSerializer() {
		ByteSerializer ser = new ByteSerializer();

		Byte val1 = 31;
		ByteBuffer bb1 = ser.toByteBuffer(val1);
		Byte val1_verify = ser.fromByteBuffer(bb1.duplicate());
		ByteBuffer bb1_str = ser.fromString("31");
		ByteBuffer bb2 = ser.getNext(bb1);
		Byte val2 = ser.fromByteBuffer(bb2);

		Assert.assertEquals(val1, val1_verify);
		Assert.assertEquals(bb1, bb1_str);
		Assert.assertEquals(1, val2.intValue() - val1.intValue());
		Assert.assertEquals(bb2.capacity(), bb1.capacity());

		ByteBuffer bbMax = ser.toByteBuffer(Byte.MAX_VALUE);
		try {
			ser.getNext(bbMax);
			Assert.fail();
		} catch (Exception e) {
			LOG.info(e.getMessage());
		}
	}
	
	@Test
	public void testShortSerializer() {
		ShortSerializer ser = new ShortSerializer();

		Short val1 = 127;
		ByteBuffer bb1 = ser.toByteBuffer(val1);
		Short val1_verify = ser.fromByteBuffer(bb1.duplicate());
		ByteBuffer bb1_str = ser.fromString("127");
		ByteBuffer bb2 = ser.getNext(bb1);
		Short val2 = ser.fromByteBuffer(bb2);

		Assert.assertEquals(val1, val1_verify);
		Assert.assertEquals(bb1, bb1_str);
		Assert.assertEquals(1, val2.intValue() - val1.intValue());
		Assert.assertEquals(bb2.capacity(), bb1.capacity());

		ByteBuffer bbMax = ser.toByteBuffer(Short.MAX_VALUE);
		try {
			ser.getNext(bbMax);
			Assert.fail();
		} catch (Exception e) {
			LOG.info(e.getMessage());
		}
	}

	@Test
	public void testStringSerializer() {
		StringSerializer ser = new StringSerializer();
	}

	@Test
	public void testUUIDSerializer() {
		UUIDSerializer ser = new UUIDSerializer();
	}

	@Test
	public void intVsBigInt() {
		IntegerSerializer intSer = new IntegerSerializer();
		BigIntegerSerializer bigIntSer = new BigIntegerSerializer();

		int value = 1234;
		intSer.toBytes(value);

		// bigIntSer.toBytes();
	}

	static class Composite1 {
		@Component
		public String firstName;

		@Component
		public String lastName;

		@Component
		public Integer age;
		
		@Component
		public BigDecimal decimal;
		
		@Component
		public BigInteger integer;

		public Composite1() {

		}

		public Composite1(String firstName, String lastName, Integer age, BigInteger bi, BigDecimal bd) {
			this.firstName = firstName;
			this.lastName = lastName;
			this.age = age;
			this.decimal = bd;
			this.integer = bi;
		}

		public String toString() {
			return new StringBuilder().append("(").append(firstName)
					.append(",").append(lastName).append(",").append(age)
					.append(")").toString();
		}

		@Override
		public boolean equals(Object arg0) {
			if (!(arg0 instanceof Composite1)) {
				return false;
			}
			Composite1 other = (Composite1) arg0;
			return (   String.valueOf(firstName).equals(String.valueOf(other.firstName))
                    && String.valueOf(lastName).equals(String.valueOf(other.lastName)) 
                    && String.valueOf(decimal).equals(String.valueOf(other.decimal)) 
                    && String.valueOf(integer).equals(String.valueOf(other.integer)) 
					&& age == other.age);
		}
	}

	@Test
	public void testAnnotatedCompositeSerializer() {
		try {
			AnnotatedCompositeSerializer<Composite1> ser = new AnnotatedCompositeSerializer<Composite1>(
					Composite1.class);

			Composite1 c1 = new Composite1("Arielle", "Landau", 6, new BigInteger("1"), new BigDecimal(1));

			ByteBuffer bytes = ser.toByteBuffer(c1);
			Composite1 c2 = ser.fromByteBuffer(bytes);
			Assert.assertEquals(c1, c2);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			Assert.fail();
		}
	}
	
	@Test
	public void testStressAnnotatedCompositeSerializer() throws Exception {
        AnnotatedCompositeSerializer<Composite1> ser = new AnnotatedCompositeSerializer<Composite1>(
                Composite1.class);

        int count = 10000;
        
        Composite1 c1 = new Composite1("Arielle", "Landau", 6, new BigInteger("1"), new BigDecimal(2));
        
        long startTime, runTime;
        
        for (int j = 0; j < 3; j++) {
            System.out.println("-----");
            
            startTime = System.nanoTime();
            for (int i = 0; i < count; i++) {
                ByteBuffer bb = ByteBuffer.allocate(8092);
                bb.putShort((short) c1.firstName.length());
                bb.put(ByteBuffer.wrap(c1.firstName.getBytes()));
                bb.put((byte) 0x00);
                
                bb.putShort((short) c1.lastName.length());
                bb.put(ByteBuffer.wrap(c1.lastName.getBytes()));
                bb.put((byte) 0x00);
        
                bb.putShort((short)4);
                bb.putInt(c1.age);
                bb.put((byte) 0x00);
                
                bb.flip();
            }
            runTime = System.nanoTime() - startTime;
            System.out.println("Raw Time in msec : " + runTime/1000000);
            
            StringSerializer sser = StringSerializer.get();
            
            Field fFirstName = Composite1.class.getField("firstName");
            Field fLastName  = Composite1.class.getField("lastName");
            Field fAge       = Composite1.class.getField("age");
            
            startTime = System.nanoTime();
            for (int i = 0; i < count; i++) {
                ByteBuffer bb = ByteBuffer.allocate(8092);
                
                String firstName = (String)fFirstName.get(c1);
                bb.putShort((short) firstName.length());
                bb.put(firstName.getBytes());
                bb.put((byte) 0x00);
                
                String lastName = (String)fLastName.get(c1);
                bb.putShort((short) lastName.length());
                bb.put(lastName.getBytes());
                bb.put((byte) 0x00);
        
                int age = (Integer)fAge.get(c1);
                bb.putShort((short)4);
                bb.putInt(age);
                bb.put((byte) 0x00);
                
                bb.flip();
            }
            runTime = System.nanoTime() - startTime;
            System.out.println("Reflection Time in msec : " + runTime/1000000);
            
            startTime = System.nanoTime();
            for (int i = 0; i < count; i++) {
                ByteBuffer bb = ser.toByteBuffer(c1);
    //            Composite1 c2 = ser.fromByteBuffer(bytes);
    //            System.out.println(Hex.encodeHexString(bb.array()));
            }
            
            runTime = System.nanoTime() - startTime;
            System.out.println("toByteBuffer Time in msec : " + runTime/1000000);
        }
	}

	@Test
	public void testAnnotatedCompositeSerializerWithNulls() {
		try {
			AnnotatedCompositeSerializer<Composite1> ser = new AnnotatedCompositeSerializer<Composite1>(
					Composite1.class);

			Composite1 c1 = new Composite1("Arielle", null, null, null, null);

			ByteBuffer bytes = ser.toByteBuffer(c1);
			Composite1 c2 = ser.fromByteBuffer(bytes);
			Assert.assertEquals(c1, c2);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			Assert.fail();
		}
	}
	
	@Test
	public void testAnnotatedCompositeSerializerWithGenerics(){
	    try{
	        AnnotatedCompositeSerializer<GenericComposite> ser = new AnnotatedCompositeSerializer<GenericComposite>(GenericComposite.class);
	        GenericComposite<String, String> c1 = new GenericComposite<String, String>("foo","bar");
	        
	        ByteBuffer bytes = ser.toByteBuffer(c1);
	        GenericComposite<String, String> c2 = ser.fromByteBuffer(bytes);
	        Assert.assertEquals(c1, c2);
	    } catch (Exception e){
	        e.printStackTrace();
	        LOG.error(e.getMessage());
	        Assert.fail();
	    }
	}
	
	static class GenericComposite<A, B>{
	    @Component(ordinal = 0)
	    A a;
	    
	    @Component(ordinal = 1)
	    B b;
	    
	    public GenericComposite(){}
	    
	    public GenericComposite(A a, B b){
	        this.a = a;
	        this.b = b;
	    }
	    
	    @Override
	    public boolean equals(Object arg0){
	        if(!(arg0 instanceof GenericComposite)){
	            return false;
	        }
	        GenericComposite other = (GenericComposite)arg0;
	        return a.equals(other.a) && b.equals(other.b); 
	    }
	}

	static class Composite2 {
		@Component(ordinal = 0)
		String firstName;

		@Component(ordinal = 2)
		String lastName;

		@Component(ordinal = 1)
		Integer age;

		public Composite2() {

		}

		public Composite2(String firstName, String lastName, Integer age) {
			this.firstName = firstName;
			this.lastName = lastName;
			this.age = age;
		}

		public String toString() {
			return new StringBuilder().append("(").append(firstName)
					.append(",").append(lastName).append(",").append(age)
					.append(")").toString();
		}

		@Override
		public boolean equals(Object arg0) {
			if (!(arg0 instanceof Composite2)) {
				return false;
			}
			Composite2 other = (Composite2) arg0;
			return (firstName.equals(other.firstName)
					&& lastName.equals(other.lastName) && age == other.age);
		}
	}

	@Test
	public void testAnnotatedCompositeSerializerWithOrdinal() {
		AnnotatedCompositeSerializer<Composite2> ser = new AnnotatedCompositeSerializer<Composite2>(
				Composite2.class);

		try {
			Composite2 c1 = new Composite2("Arielle", "Landau", 6);
			ByteBuffer bytes = ser.toByteBuffer(c1);
			Composite2 c2 = ser.fromByteBuffer(bytes);
			Composite2 c3 = ser.fromByteBuffer(bytes);

			Assert.assertEquals(c1, c2);
			Assert.assertEquals(c2, c3);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			Assert.fail();
		}
	}

	@Test
	public void testCompositeType() {
		String comparatorType = "CompositeType(UTF8Type,UTF8Type)";
		String columnName = "(abc,1234)";
		try {
			AbstractType type = TypeParser.parse(comparatorType);
			if (type instanceof CompositeType) {
				CompositeType ctype = (CompositeType) type;

				ByteBuffer data = ctype.fromString(columnName);
				String columnName2 = ctype.getString(data);
				Assert.assertEquals(columnName, columnName2);
			} else {
				Assert.fail();
			}
		} catch (Exception e) {
			Assert.fail();
			LOG.error(e.getMessage());
		}
	}

	@Test
	public void testDeserializeOfSepecificSerializer() throws Exception
	{
		Composite composite1 = new Composite( "abc", 123L );
		CompositeSerializer serializer = new SpecificCompositeSerializer(
				( CompositeType )TypeParser.parse( "CompositeType(UTF8Type,LongType)" ) );
		ByteBuffer byteBuffer = serializer.toByteBuffer( composite1 );
		Composite composite2 = serializer.fromByteBuffer( byteBuffer );

		Assert.assertEquals(String.class, composite1.getComponent( 0 ).getValue().getClass());
		Assert.assertEquals(Long.class, composite1.getComponent( 1 ).getValue().getClass());
		
		Assert.assertEquals( composite1.getComponent( 0 ).getValue().getClass(), composite2.getComponent( 0 ).getValue().getClass() );
		Assert.assertEquals( composite1.getComponent( 1 ).getValue().getClass(), composite2.getComponent( 1 ).getValue().getClass() );
		
		Assert.assertEquals( composite1.getComponent( 0 ).getValue(), composite2.getComponent( 0 ).getValue() );
		Assert.assertEquals( composite1.getComponent( 1 ).getValue(), composite2.getComponent( 1 ).getValue() );
	}
}

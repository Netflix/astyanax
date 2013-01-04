package com.netflix.astyanax.entitystore;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Id;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Charsets;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.ComparatorType;

/**
 * id is not counted as column
 * 17 columns
 */
public class SampleEntity {

	public static class Foo {

		public int i;
		public String s;

		public Foo(int i, String s) {
			this.i = i;
			this.s = s;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Foo other = (Foo) obj;
			if(i == other.i && s.equals(other.s))
				return true;
			else
				return false;
		}

		@Override
		public String toString() {		
			try {
				JSONObject jsonObj = new JSONObject();
				jsonObj.put("i", i);
				jsonObj.put("s", s);
				return jsonObj.toString();
			} catch (JSONException e) {
				throw new RuntimeException("failed to construct JSONObject for toString", e);
			}
		}

		public static Foo fromString(String str) {
			try {
				JSONObject jsonObj = new JSONObject(str);
				return new Foo(jsonObj.getInt("i"), jsonObj.getString("s"));
			} catch (JSONException e) {
				throw new RuntimeException("failed to construct JSONObject for toString", e);
			}			
		}
	}

	public static class FooSerializer extends AbstractSerializer<Foo> {

		private static final String UTF_8 = "UTF-8";
		private static final Charset charset = Charset.forName(UTF_8);
		private static final FooSerializer instance = new FooSerializer();

		public static FooSerializer get() {
			return instance;
		}

		@Override
		public ByteBuffer toByteBuffer(Foo obj) {
			if (obj == null) {
				return null;
			}
			return ByteBuffer.wrap(obj.toString().getBytes(charset));
		}

		@Override
		public Foo fromByteBuffer(ByteBuffer byteBuffer) {
			if (byteBuffer == null) {
				return null;
			}
			return Foo.fromString(charset.decode(byteBuffer).toString());
		}

		@Override
		public ComparatorType getComparatorType() {
			return ComparatorType.UTF8TYPE;
		}

		@Override
		public ByteBuffer fromString(String str) {
			return UTF8Type.instance.fromString(str);
		}

		@Override
		public String getString(ByteBuffer byteBuffer) {
			return UTF8Type.instance.getString(byteBuffer);
		}
	}

	@Id
	private String id;

	@Column(name="BOOLEAN_PRIMITIVE")
	private boolean booleanPrimitive;

	@Column(name="BOOLEAN_OBJECT")
	private Boolean booleanObject;

	@Column(name="BYTE_PRIMITIVE")
	private byte bytePrimitive;

	@Column(name="BYTE_OBJECT")
	private Byte byteObject;

	@Column(name="SHORT_PRIMITIVE")
	private short shortPrimitive;

	@Column(name="SHORT_OBJECT")
	private Short shortObject;

	@Column(name="INT_PRIMITIVE")
	private int intPrimitive;

	@Column(name="INT_OBJECT")
	private Integer intObject;

	@Column(name="LONG_PRIMITIVE")
	private long longPrimitive;

	@Column(name="LONG_OBJECT")
	private Long longObject;

	@Column(name="FLOAT_PRIMITIVE")
	private float floatPrimitive;

	@Column(name="FLOAT_OBJECT")
	private Float floatObject;

	@Column(name="DOUBLE_PRIMITIVE")
	private double doublePrimitive;

	@Column(name="DOUBLE_OBJECT")
	private Double doubleObject;

	@Column(name="STRING")
	private String string;

	@Column(name="BYTE_ARRAY")
	@TTL(123456)
	private byte[] byteArray;

	@Column(name="DATE")
	private Date date;

	// name should default to field name
	@Column()
	private UUID uuid;

	@Column(name="FOO")
	@Serializer(FooSerializer.class)
	private Foo foo;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean isBooleanPrimitive() {
		return booleanPrimitive;
	}

	public void setBooleanPrimitive(boolean booleanPrimitive) {
		this.booleanPrimitive = booleanPrimitive;
	}

	public Boolean getBooleanObject() {
		return booleanObject;
	}

	public void setBooleanObject(Boolean booleanObject) {
		this.booleanObject = booleanObject;
	}

	public byte getBytePrimitive() {
		return bytePrimitive;
	}

	public void setBytePrimitive(byte bytePrimitive) {
		this.bytePrimitive = bytePrimitive;
	}

	public Byte getByteObject() {
		return byteObject;
	}

	public void setByteObject(Byte byteObject) {
		this.byteObject = byteObject;
	}

	public short getShortPrimitive() {
		return shortPrimitive;
	}

	public void setShortPrimitive(short shortPrimitive) {
		this.shortPrimitive = shortPrimitive;
	}

	public Short getShortObject() {
		return shortObject;
	}

	public void setShortObject(Short shortObject) {
		this.shortObject = shortObject;
	}

	public int getIntPrimitive() {
		return intPrimitive;
	}

	public void setIntPrimitive(int intPrimitive) {
		this.intPrimitive = intPrimitive;
	}

	public Integer getIntObject() {
		return intObject;
	}

	public void setIntObject(Integer intObject) {
		this.intObject = intObject;
	}

	public long getLongPrimitive() {
		return longPrimitive;
	}

	public void setLongPrimitive(long longPrimitive) {
		this.longPrimitive = longPrimitive;
	}

	public Long getLongObject() {
		return longObject;
	}

	public void setLongObject(Long longObject) {
		this.longObject = longObject;
	}

	public float getFloatPrimitive() {
		return floatPrimitive;
	}

	public void setFloatPrimitive(float floatPrimitive) {
		this.floatPrimitive = floatPrimitive;
	}

	public Float getFloatObject() {
		return floatObject;
	}

	public void setFloatObject(Float floatObject) {
		this.floatObject = floatObject;
	}

	public double getDoublePrimitive() {
		return doublePrimitive;
	}

	public void setDoublePrimitive(double doublePrimitive) {
		this.doublePrimitive = doublePrimitive;
	}

	public Double getDoubleObject() {
		return doubleObject;
	}

	public void setDoubleObject(Double doubleObject) {
		this.doubleObject = doubleObject;
	}

	public String getString() {
		return string;
	}

	public void setString(String string) {
		this.string = string;
	}

	public byte[] getByteArray() {
		return byteArray;
	}

	public void setByteArray(byte[] byteArray) {
		this.byteArray = byteArray;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}

	public Foo getFoo() {
		return foo;
	}

	public void setFoo(Foo foo) {
		this.foo = foo;
	}

	@Override
	public String toString() {
		return String.format("SampleEntity(id = %s, booleanPrimitive = %b, booleanObject = %b, " +
				"bytePrimitive = %d, byteObject = %d, shortPrimitive = %d, shortObject = %d, " +
				"intPrimitive = %d, intObject = %d, longPrimitive = %d, longObject = %d, " +
				"floatPrimitive = %f, floatObject = %f, doublePrimitive = %f, doubleObject = %f, " +
				"string = %s, byteArray = %s, date = %s, uuid = %s, " +
				"foo = %s)", 
				id, booleanPrimitive, booleanObject, 
				bytePrimitive, byteObject, shortPrimitive, shortObject, 
				intPrimitive, intObject, longPrimitive, longObject,
				floatPrimitive, floatObject, doublePrimitive, doubleObject,
				string, new String(byteArray, Charsets.UTF_8), date, uuid, 
				foo);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;

		if (obj == null)
			return false;

		if (getClass() != obj.getClass())
			return false;

		SampleEntity other = (SampleEntity) obj;
		if(id.equals(other.id) &&
				booleanPrimitive == other.booleanPrimitive &&
				booleanObject.equals(other.booleanObject) &&
				bytePrimitive == other.bytePrimitive &&
				byteObject.equals(other.byteObject) &&
				shortPrimitive == other.shortPrimitive &&
				shortObject.equals(other.shortObject) &&
				intPrimitive == other.intPrimitive &&
				intObject.equals(other.intObject) &&
				longPrimitive == other.longPrimitive &&
				longObject.equals(other.longObject) &&
				floatPrimitive == other.floatPrimitive &&
				floatObject.equals(other.floatObject) &&
				doublePrimitive == other.doublePrimitive &&
				doubleObject.equals(other.doubleObject) &&
				string.equals(other.string) &&
				Arrays.equals(byteArray, other.byteArray) &&
				date.equals(other.date) &&
				uuid.equals(other.uuid) &&
				foo.equals(other.foo)
				)
			return true;
		else
			return false;
	}
}

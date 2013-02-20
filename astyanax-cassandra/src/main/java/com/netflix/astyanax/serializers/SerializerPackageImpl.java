package com.netflix.astyanax.serializers;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

//import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.commons.lang.StringUtils;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;

/**
 * Basic implementation of SerializerPackage which can be configured either from
 * a ColumnFamilyDefinition or by manually setting either the ComparatorType or
 * Serializer for keys, columns and values. Use this in conjunction with the CSV
 * uploader to specify how values are serializer.
 * 
 * @author elandau
 * 
 */
public class SerializerPackageImpl implements SerializerPackage {

    private final static Serializer<?> DEFAULT_SERIALIZER = BytesArraySerializer.get();

    public final static SerializerPackage DEFAULT_SERIALIZER_PACKAGE = new SerializerPackageImpl();

    private Serializer<?> keySerializer = DEFAULT_SERIALIZER;
    private Serializer<?> columnSerializer = DEFAULT_SERIALIZER;
    private Serializer<?> defaultValueSerializer = DEFAULT_SERIALIZER;

    private final Map<ByteBuffer, Serializer<?>> valueSerializers = new HashMap<ByteBuffer, Serializer<?>>();;

    public SerializerPackageImpl() {
    }

    /**
     * Construct a serializer package from a column family definition retrieved
     * from the keyspace. This is the preferred method of initialing the
     * serializer since it most closely matches the validators and comparator
     * type set in cassandra.
     * 
     * @param cfDef
     * @param ignoreErrors
     * @throws UnknownComparatorException
     */
    public SerializerPackageImpl(ColumnFamilyDefinition cfDef, boolean ignoreErrors) throws UnknownComparatorException {
        try {
            setKeyType(cfDef.getKeyValidationClass());
        }
        catch (UnknownComparatorException e) {
            if (!ignoreErrors)
                throw e;
        }

        try {
            setColumnType(cfDef.getComparatorType());
        }
        catch (UnknownComparatorException e) {
            if (!ignoreErrors)
                throw e;
        }

        try {
            setDefaultValueType(cfDef.getDefaultValidationClass());
        }
        catch (UnknownComparatorException e) {
            if (!ignoreErrors)
                throw e;
        }

        List<ColumnDefinition> colsDefs = cfDef.getColumnDefinitionList();
        if (colsDefs != null) {
            for (ColumnDefinition colDef : colsDefs) {
                try {
                    this.setValueType(colDef.getRawName(), colDef.getValidationClass());
                }
                catch (UnknownComparatorException e) {
                    if (!ignoreErrors)
                        throw e;
                }
            }
        }
    }

    public SerializerPackageImpl setKeyType(String keyType) throws UnknownComparatorException {
        String comparatorType = StringUtils.substringBefore(keyType, "(");
        ComparatorType type = ComparatorType.getByClassName(comparatorType);

        if (type == null) {
            throw new UnknownComparatorException(keyType);
        }

        if (type == ComparatorType.COMPOSITETYPE) {
            try {
                this.keySerializer = new SpecificCompositeSerializer((CompositeType) TypeParser.parse(keyType));
                return this;
            }
            catch (Exception e) {
                // Ignore and simply use the default serializer
            }
            throw new UnknownComparatorException(keyType);
        }
        else if (type == ComparatorType.DYNAMICCOMPOSITETYPE) {
            // TODO
            throw new UnknownComparatorException(keyType);
        }
        else {
            this.keySerializer = type.getSerializer();
        }

        return this;
    }

    public SerializerPackageImpl setKeySerializer(Serializer<?> serializer) {
        this.keySerializer = serializer;
        return this;
    }

    @Deprecated
    public SerializerPackageImpl setColumnType(String columnType) throws UnknownComparatorException {
        return setColumnNameType(columnType);
    }

    public SerializerPackageImpl setColumnNameType(String columnType) throws UnknownComparatorException {
        // Determine the column serializer
        String comparatorType = StringUtils.substringBefore(columnType, "(");
        ComparatorType type = ComparatorType.getByClassName(comparatorType);
        if (type == null) {
            throw new UnknownComparatorException(columnType);
        }

        if (type == ComparatorType.COMPOSITETYPE) {
            try {
                this.columnSerializer = new SpecificCompositeSerializer((CompositeType) TypeParser.parse(columnType));
                return this;
            }
            catch (Exception e) {
                // Ignore and simply use the default serializer
            }
            throw new UnknownComparatorException(columnType);
        }
        else if (type == ComparatorType.DYNAMICCOMPOSITETYPE) {
            // TODO
            throw new UnknownComparatorException(columnType);
        }
        else {
            this.columnSerializer = type.getSerializer();
        }
        return this;
    }

    public SerializerPackageImpl setColumnNameSerializer(Serializer<?> serializer) {
        this.columnSerializer = serializer;
        return this;
    }

    public SerializerPackageImpl setDefaultValueType(String valueType) throws UnknownComparatorException {
        // Determine the VALUE serializer. There is always a default serializer
        // and potentially column specific serializers
        ComparatorType type = ComparatorType.getByClassName(valueType);
        if (type == null) {
            throw new UnknownComparatorException(valueType);
        }
        this.defaultValueSerializer = type.getSerializer();
        return this;
    }

    public SerializerPackageImpl setDefaultValueSerializer(Serializer<?> serializer) {
        this.defaultValueSerializer = serializer;
        return this;
    }

    public SerializerPackageImpl setValueType(String columnName, String type) throws UnknownComparatorException {
        setValueType(StringSerializer.get().toByteBuffer(columnName), type);
        return this;
    }

    public SerializerPackageImpl setValueType(ByteBuffer columnName, String valueType)
            throws UnknownComparatorException {
        ComparatorType type = ComparatorType.getByClassName(valueType);
        if (type == null) {
            throw new UnknownComparatorException(valueType);
        }
        this.valueSerializers.put(columnName, type.getSerializer());
        return this;
    }

    public SerializerPackageImpl setValueSerializer(String columnName, Serializer<?> serializer) {
        this.valueSerializers.put(StringSerializer.get().toByteBuffer(columnName), serializer);
        return this;
    }

    public SerializerPackageImpl setValueSerializer(ByteBuffer columnName, Serializer<?> serializer) {
        this.valueSerializers.put(columnName, serializer);
        return this;
    }

    @Override
    public Serializer<?> getKeySerializer() {
        return this.keySerializer;
    }

    @Override
    @Deprecated
    public Serializer<?> getColumnSerializer() {
        return getColumnNameSerializer();
    }

    @Override
    public Serializer<?> getColumnNameSerializer() {
        return this.columnSerializer;
    }

    @Override
    @Deprecated
    public Serializer<?> getValueSerializer(ByteBuffer columnName) {
        return getColumnSerializer(columnName);
    }

    public Serializer<?> getColumnSerializer(ByteBuffer columnName) {
        if (valueSerializers == null) {
            return defaultValueSerializer;
        }

        Serializer<?> ser = valueSerializers.get(columnName);
        if (ser == null) {
            return defaultValueSerializer;
        }
        return ser;
    }

    @Override
    public Serializer<?> getColumnSerializer(String columnName) {
        return getValueSerializer(StringSerializer.get().toByteBuffer(columnName));
    }

    @Override
    @Deprecated
    public Serializer<?> getValueSerializer(String columnName) {
        return getColumnSerializer(columnName);
    }

    @Override
    public Set<ByteBuffer> getColumnNames() {
        Set<ByteBuffer> set = new HashSet<ByteBuffer>();
        if (valueSerializers != null) {
            for (Entry<ByteBuffer, Serializer<?>> entry : valueSerializers.entrySet()) {
                set.add(entry.getKey().duplicate());
            }
        }
        return set;
    }

    @Override
    @Deprecated
    public Serializer<?> getValueSerializer() {
        return getDefaultValueSerializer();
    }

    @Override
    public Serializer<?> getDefaultValueSerializer() {
        return defaultValueSerializer;
    }

    @Override
    public String keyAsString(ByteBuffer key) {
        return this.keySerializer.getString(key);
    }

    @Override
    public String columnAsString(ByteBuffer column) {
        return this.columnSerializer.getString(column);
    }

    @Override
    public String valueAsString(ByteBuffer column, ByteBuffer value) {
        Serializer<?> serializer = this.valueSerializers.get(column);
        if (serializer == null) {
            return this.defaultValueSerializer.getString(value);
        }
        else {
            return serializer.getString(value);
        }
    }

    @Override
    public ByteBuffer keyAsByteBuffer(String key) {
        return this.keySerializer.fromString(key);
    }

    @Override
    public ByteBuffer columnAsByteBuffer(String column) {
        return this.columnSerializer.fromString(column);
    }

    @Override
    public ByteBuffer valueAsByteBuffer(ByteBuffer column, String value) {
        Serializer<?> serializer = this.valueSerializers.get(column);
        if (serializer == null) {
            return this.defaultValueSerializer.fromString(value);
        }
        else {
            return serializer.fromString(value);
        }
    }

    @Override
    public ByteBuffer valueAsByteBuffer(String column, String value) {
        return valueAsByteBuffer(this.columnSerializer.fromString(column), value);
    }
}

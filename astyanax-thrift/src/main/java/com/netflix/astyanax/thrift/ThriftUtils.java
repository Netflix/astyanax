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
package com.netflix.astyanax.thrift;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TEnum;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.Serializer;

public class ThriftUtils {
    private final static Logger LOG = LoggerFactory.getLogger(ThriftUtils.class);
    
    public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);
//    private static final SliceRange RANGE_ALL = new SliceRange(EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE);
    public static final int MUTATION_OVERHEAD = 20;

    public static SliceRange createAllInclusiveSliceRange() {
        return new SliceRange(EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE);
    }
    
    public static <C> SliceRange createSliceRange(Serializer<C> serializer, C startColumn, C endColumn,
            boolean reversed, int limit) {
        return new SliceRange((startColumn == null) ? EMPTY_BYTE_BUFFER : serializer.toByteBuffer(startColumn),
                (endColumn == null) ? EMPTY_BYTE_BUFFER : serializer.toByteBuffer(endColumn), reversed, limit);

    }
    
    public static <T extends org.apache.thrift.TBase> Properties getPropertiesFromThrift(T entity) throws Exception {
        Properties props = new Properties();
        
        setPropertiesFromThrift("", props, entity);
        return props;
    }
    
    /**
     * Quick and dirty implementation that converts thrift DDL to a Properties object by flattening
     * the parameters
     * @param prefix
     * @param properties
     * @param entity
     * @throws Exception
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void setPropertiesFromThrift(String prefix, Properties properties, org.apache.thrift.TBase entity) throws Exception {
        Field field = entity.getClass().getDeclaredField("metaDataMap");
        Map<org.apache.thrift.TFieldIdEnum, org.apache.thrift.meta_data.FieldMetaData> fields = (Map<org.apache.thrift.TFieldIdEnum, FieldMetaData>) field.get(entity);
        
        for (Entry<org.apache.thrift.TFieldIdEnum, FieldMetaData> f : fields.entrySet()) { 
            ThriftTypes type = ThriftTypes.values()[f.getValue().valueMetaData.type];
            Object value = entity.getFieldValue(f.getKey());
            if (value == null)
                continue;
            
            switch (type) {
            case VOID   : 
                break;
            case BOOL   :
            case BYTE   :
            case DOUBLE :
            case I16    :
            case I32    :
            case I64    :
            case STRING :
            case ENUM   : 
                if (value instanceof byte[]) {
                    properties.put(prefix + f.getKey().getFieldName(), Base64.encodeBase64String((byte[])value));
                }
                else if (value instanceof ByteBuffer) {
                    properties.put(prefix + f.getKey().getFieldName(), base64Encode((ByteBuffer)value));
                }
                else {
                    properties.put(prefix + f.getKey().getFieldName(), value.toString());
                }
                break;
            case MAP    : {
                String newPrefix = prefix + f.getKey().getFieldName() + ".";
                org.apache.thrift.meta_data.MapMetaData meta = (org.apache.thrift.meta_data.MapMetaData)f.getValue().valueMetaData;
                if (!meta.keyMetaData.isStruct() && !meta.keyMetaData.isContainer()) {
                    Map<Object, Object> map = (Map<Object, Object>)value;
                    for (Entry<Object, Object> entry : map.entrySet()) {
                        properties.put(newPrefix + entry.getKey(), entry.getValue().toString());
                    }
                }
                else {
                    LOG.error(String.format("Unable to serializer field '%s' key type '%s' not supported", f.getKey().getFieldName(), meta.keyMetaData.getTypedefName()));
                }
                break;
            }
            case LIST   : {
                String newPrefix = prefix + f.getKey().getFieldName() + ".";

                List<Object> list = (List<Object>)value;
                org.apache.thrift.meta_data.ListMetaData listMeta = (org.apache.thrift.meta_data.ListMetaData)f.getValue().valueMetaData;
                for (Object entry : list) {
                    String id;
                    if (entry instanceof CfDef) {
                        id = ((CfDef)entry).name;
                    }
                    else if (entry instanceof ColumnDef) {
                        ByteBuffer name = ((ColumnDef)entry).name;
                        id = base64Encode(name);
                    }
                    else {
                        LOG.error("Don't know how to convert to properties " + listMeta.elemMetaData.getTypedefName());
                        continue;
                    }
                    
                    if (listMeta.elemMetaData.isStruct()) {
                        setPropertiesFromThrift(newPrefix + id + ".", properties, (org.apache.thrift.TBase)entry);
                    }
                    else {
                        properties.put(newPrefix + id, entry);
                    }
                }
                
                break;
            }
            case STRUCT : {
                setPropertiesFromThrift(prefix + f.getKey().getFieldName() + ".", properties, (org.apache.thrift.TBase)value);
                break;
            }
            case SET    :
            default:
                LOG.error("Unhandled value : " + f.getKey().getFieldName() + " " + type);
                break;
            }
        }
    }
    
    private static String base64Encode(ByteBuffer bb) {
        if (bb == null) {
            return "";
        }
        byte[] nbb = new byte[bb.remaining()];
        bb.duplicate().get(nbb, 0, bb.remaining());
        return Base64.encodeBase64String(nbb);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T> T getThriftObjectFromProperties(Class<T> clazz, Properties props) throws Exception {
        org.apache.thrift.TBase entity = (org.apache.thrift.TBase)clazz.newInstance();
        return (T)populateObjectFromProperties(entity, props);
    }
    
    public static Object populateObjectFromProperties(Object entity, Properties props) throws Exception {
        return populateObject(entity, propertiesToMap(props));
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static Object populateObject(Object obj, Map<String, Object> map) throws Exception {
        org.apache.thrift.TBase entity = (org.apache.thrift.TBase)obj;
        Field field = entity.getClass().getDeclaredField("metaDataMap");
        Map<org.apache.thrift.TFieldIdEnum, org.apache.thrift.meta_data.FieldMetaData> fields = (Map<org.apache.thrift.TFieldIdEnum, FieldMetaData>) field.get(entity);

        for (Entry<TFieldIdEnum, FieldMetaData> f : fields.entrySet()) {
            Object value = map.get(f.getKey().getFieldName());
            if (value != null) {
                ThriftTypes type = ThriftTypes.values()[f.getValue().valueMetaData.type];
                
                switch (type) {
                case VOID   : 
                    break;
                case BYTE   :
                case BOOL   :
                case DOUBLE :
                case I16    :
                case I32    :
                case I64    :
                case STRING :
                    try {
                        entity.setFieldValue(f.getKey(), valueForBasicType(value, f.getValue().valueMetaData.type));
                    }
                    catch (ClassCastException e) {
                        if (e.getMessage().contains(ByteBuffer.class.getCanonicalName())) {
                            entity.setFieldValue(f.getKey(), ByteBuffer.wrap(Base64.decodeBase64((String)value)));
                        }
                        else {
                            throw e;
                        }
                    }
                    break;
                case ENUM   : {
                    org.apache.thrift.meta_data.EnumMetaData meta = (org.apache.thrift.meta_data.EnumMetaData)f.getValue().valueMetaData;
                    Object e = meta.enumClass;
                    entity.setFieldValue(f.getKey(), Enum.valueOf((Class<Enum>) e, (String)value));
                    break;
                }
                case MAP    : {
                    org.apache.thrift.meta_data.MapMetaData meta = (org.apache.thrift.meta_data.MapMetaData)f.getValue().valueMetaData;
                    if (!meta.keyMetaData.isStruct() && !meta.keyMetaData.isContainer()) {
                        Map<Object, Object> childMap      = (Map<Object, Object>)value;
                        Map<Object, Object> childEntityMap = Maps.newHashMap();
                        entity.setFieldValue(f.getKey(), childEntityMap);
                        
                        if (!meta.keyMetaData.isStruct() && !meta.keyMetaData.isContainer()) {
                            for (Entry<Object, Object> entry : childMap.entrySet()) {
                                Object childKey   = valueForBasicType(entry.getKey(),   meta.keyMetaData.type);
                                Object childValue = valueForBasicType(entry.getValue(), meta.valueMetaData.type);
                                childEntityMap.put(childKey, childValue);
                            }
                        }
                    }
                    else {
                        LOG.error(String.format("Unable to serializer field '%s' key type '%s' not supported", f.getKey().getFieldName(), meta.keyMetaData.getTypedefName()));
                    }
                    break;
                }
                case LIST   : {
                    Map<String, Object> childMap = (Map<String, Object>)value;
                    org.apache.thrift.meta_data.ListMetaData listMeta = (org.apache.thrift.meta_data.ListMetaData)f.getValue().valueMetaData;
                    
                    // Create an empty list and attach to the parent entity
                    List<Object> childList = Lists.newArrayList();
                    entity.setFieldValue(f.getKey(), childList);
                    
                    if (listMeta.elemMetaData instanceof org.apache.thrift.meta_data.StructMetaData) {
                        org.apache.thrift.meta_data.StructMetaData structMeta = (org.apache.thrift.meta_data.StructMetaData)listMeta.elemMetaData;
                        for (Entry<String, Object> childElement : childMap.entrySet()) {
                            org.apache.thrift.TBase childEntity = structMeta.structClass.newInstance();
                            populateObject(childEntity, (Map<String, Object>)childElement.getValue());
                            childList.add(childEntity);
                        }
                    }
                    break;
                }
                case STRUCT : {
                    break;
                }
                case SET    :
                default:
                    LOG.error("Unhandled value : " + f.getKey().getFieldName() + " " + type);
                    break;
                }
            }
        }
        return entity;
    }
        
    public static Object valueForBasicType(Object value, byte type) {
        switch (ThriftTypes.values()[type]) {
        case BYTE   :
            return Byte.parseByte((String)value);
        case BOOL   :
            return Boolean.parseBoolean((String)value);
        case DOUBLE :
            return Double.parseDouble((String)value);
        case I16    :
            return Short.parseShort((String)value);
        case I32    :
            return Integer.parseInt((String)value);
        case I64    :
            return Long.parseLong((String)value);
        case STRING :
            return value;
        default:
            return null;
        }
    }
    
    /**
     * Convert a Properties object into a tree
     * @param props
     * @return
     */
    public static Map<String, Object> propertiesToMap(Properties props) {
        Map<String, Object> root = Maps.newTreeMap();
        for (Entry<Object, Object> prop : props.entrySet()) {
            String[] parts = StringUtils.split((String)prop.getKey(), ".");
            Map<String, Object> node = root;
            for (int i = 0; i < parts.length - 1; i++) {
                if (!node.containsKey(parts[i])) {
                    node.put(parts[i], new LinkedHashMap<String, Object>());
                }
                node = (Map<String, Object>)node.get(parts[i]);
            }
            node.put(parts[parts.length-1], (String)prop.getValue());
        }
        return root;
    }

}

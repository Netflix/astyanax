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
package com.netflix.astyanax.mapping;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.Column;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.UUID;

class Coercions {
    @SuppressWarnings("unchecked")
    static <T> void setFieldFromColumn(T instance, Field field,
            Column<String> column) {
        Object objValue;
        if ((field.getType() == Byte.class) || (field.getType() == Byte.TYPE)) {
            objValue = column.getByteValue();
        } else if ((field.getType() == Boolean.class)
                || (field.getType() == Boolean.TYPE)) {
            objValue = column.getBooleanValue();
        } else if ((field.getType() == Short.class)
                || (field.getType() == Short.TYPE)) {
            objValue = column.getShortValue();
        } else if ((field.getType() == Integer.class)
                || (field.getType() == Integer.TYPE)) {
            objValue = column.getIntegerValue();
        } else if ((field.getType() == Long.class)
                || (field.getType() == Long.TYPE)) {
            objValue = column.getLongValue();
        } else if ((field.getType() == Float.class)
                || (field.getType() == Float.TYPE)) {
            objValue = column.getFloatValue();
        } else if ((field.getType() == Double.class)
                || (field.getType() == Double.TYPE)) {
            objValue = column.getDoubleValue();
        } else if (field.getType() == Date.class) {
            objValue = column.getDateValue();
        } else if (field.getType() == String.class) {
            objValue = column.getStringValue();
        } else if (field.getType() == byte[].class) {
            objValue = column.getByteArrayValue();
        } else if (field.getType() == UUID.class) {
            objValue = column.getUUIDValue();
        } else if (field.getType().isEnum()) {
            objValue = Enum.valueOf((Class<? extends Enum>)field.getType(), column.getStringValue());
        } else {
            throw new UnsupportedOperationException();
        }

        try {
            field.set(instance, objValue);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e); // should never get here
        }
    }

    static <T> void setColumnMutationFromField(T instance, Field field,
            String columnName, ColumnListMutation<String> mutation, Integer ttl) {
        try {
            Object objValue = field.get(instance);

            if (objValue != null) {
                if ((objValue.getClass() == Byte.class)
                        || (objValue.getClass() == Byte.TYPE)) {
                    mutation.putColumn(columnName, (Byte) objValue, ttl);
                } else if ((objValue.getClass() == Boolean.class)
                        || (objValue.getClass() == Boolean.TYPE)) {
                    mutation.putColumn(columnName, (Boolean) objValue, ttl);
                } else if ((objValue.getClass() == Short.class)
                        || (objValue.getClass() == Short.TYPE)) {
                    mutation.putColumn(columnName, (Short) objValue, ttl);
                } else if ((objValue.getClass() == Integer.class)
                        || (objValue.getClass() == Integer.TYPE)) {
                    mutation.putColumn(columnName, (Integer) objValue, ttl);
                } else if ((objValue.getClass() == Long.class)
                        || (objValue.getClass() == Long.TYPE)) {
                    mutation.putColumn(columnName, (Long) objValue, ttl);
                } else if ((objValue.getClass() == Float.class)
                        || (objValue.getClass() == Float.TYPE)) {
                    mutation.putColumn(columnName, (Float) objValue, ttl);
                } else if ((objValue.getClass() == Double.class)
                        || (objValue.getClass() == Double.TYPE)) {
                    mutation.putColumn(columnName, (Double) objValue, ttl);
                } else if (objValue.getClass() == Date.class) {
                    mutation.putColumn(columnName, (Date) objValue, ttl);
                } else if (objValue.getClass() == String.class) {
                    mutation.putColumn(columnName, (String) objValue, ttl);
                } else if(objValue.getClass() == byte[].class) {
                    mutation.putColumn(columnName, (byte[]) objValue, ttl);
                } else if (objValue.getClass() == UUID.class) {
                    mutation.putColumn(columnName, (UUID) objValue, ttl);
                } else if (objValue.getClass().isEnum()) {
                    mutation.putColumn(columnName, objValue.toString(), ttl);
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e); // should never get here
        }
    }

    private Coercions() {
    }
}

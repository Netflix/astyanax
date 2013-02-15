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

import java.lang.reflect.Field;

/**
 * The default annotation set. Supports {@link Id} and {@link Column}
 */
@Deprecated
public class DefaultAnnotationSet implements AnnotationSet<Id, Column> {
    @Override
    public Class<Id> getIdAnnotation() {
        return Id.class;
    }

    @Override
    public Class<Column> getColumnAnnotation() {
        return Column.class;
    }

    @Override
    public String getIdName(Field field, Id annotation) {
        String name = annotation.value();
        return (name.length() > 0) ? name : field.getName();
    }

    @Override
    public String getColumnName(Field field, Column annotation) {
        String name = annotation.value();
        return (name.length() > 0) ? name : field.getName();
    }
}

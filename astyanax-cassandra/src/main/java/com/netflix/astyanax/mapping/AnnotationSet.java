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
package com.netflix.astyanax.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

/**
 * Allows for any annotations to be used to mark columns in a bean
 */
@Deprecated
public interface AnnotationSet<ID extends Annotation, COLUMN extends Annotation> {
    /**
     * @return the Annotation class that marks a bean field as being the ID/Key
     */
    public Class<ID> getIdAnnotation();

    /**
     * @return the Annotation class that marks a bean field as being
     *         persist-able.
     */
    public Class<COLUMN> getColumnAnnotation();

    /**
     * Return the ID/Key name to use
     * 
     * @param field
     *            the field from the bean
     * @param annotation
     *            the id annotation
     * @return name to use for the field (cannot be null)
     */
    public String getIdName(Field field, ID annotation);

    /**
     * Return the column name to use for the given field. NOTE: if the field
     * should not be persisted, return <code>null</code>.
     * 
     * @param field
     *            the field from the bean
     * @param annotation
     *            the column annotation
     * @return name to use for the field or null
     */
    public String getColumnName(Field field, COLUMN annotation);
}

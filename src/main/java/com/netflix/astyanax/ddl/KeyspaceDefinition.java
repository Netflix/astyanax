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
package com.netflix.astyanax.ddl;

import java.util.List;
import java.util.Map;

public interface KeyspaceDefinition {

    KeyspaceDefinition setName(String name);

    String getName();

    KeyspaceDefinition setStrategyClass(String strategyClass);

    String getStrategyClass();

    KeyspaceDefinition setStrategyOptions(Map<String, String> options);

    KeyspaceDefinition addStrategyOption(String name, String value);

    Map<String, String> getStrategyOptions();

    KeyspaceDefinition addColumnFamily(ColumnFamilyDefinition cfDef);

    List<ColumnFamilyDefinition> getColumnFamilyList();

    ColumnFamilyDefinition getColumnFamily(String columnFamily);

    List<String> getFieldNames();

    Object getFieldValue(String name);

    KeyspaceDefinition setFieldValue(String name, Object value);
    
    /**
     * Get metadata for all fields
     * @return
     */
    List<FieldMetadata> getFieldsMetadata();

}

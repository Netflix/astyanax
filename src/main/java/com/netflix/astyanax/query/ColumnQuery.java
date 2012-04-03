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
package com.netflix.astyanax.query;

import com.netflix.astyanax.Execution;
import com.netflix.astyanax.model.Column;

/**
 * Interface to execute a column query on a single row.
 * 
 * @author elandau
 * 
 * @param <C>
 */
public interface ColumnQuery<C> extends Execution<Column<C>> {
}

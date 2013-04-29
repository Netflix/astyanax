/* 
 * Copyright (c) 2013 Research In Motion Limited. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 */
package com.netflix.astyanax.index;

import com.netflix.astyanax.query.RowSliceQuery;

/**
 * This will allow an internal "mapping" of the key of any query that uses {@link Index}
 * 
 *  Without it, the danger is that old values of the index will not be removed
 *  {@link Index#updateIndex(Object, Object, Object, Object)}
 *   and index will resort to 
 *   {@link Index#insertIndex(Object, Object, Object)}
 *   
 *   
 *  
 * 
 * 
 * @author marcus
 *
 * @param <K> - the key type
 * @param <C> - the column type
 * @param <V> - the column value type
 */
public interface HighCardinalityQuery<K, C, V>  {

	
	/**
	 * A wrapped version of the row slice query.
	 * 
	 * @param name
	 * @param value
	 * @return
	 */
	RowSliceQuery<K, C> equals(C name, V value);
	
	//IndexRead<K,C,V> readIndex();
	/**
	 * Register a repair listener that will be informed of changes
	 * when reading from index CF that doesn't make map back to
	 * the original CF that contains the indexed values - which are considered
	 * master values 
	 * 
	 * @param repairListener
	 */
	void registerRepairListener(RepairListener<K, C, V> repairListener);
}

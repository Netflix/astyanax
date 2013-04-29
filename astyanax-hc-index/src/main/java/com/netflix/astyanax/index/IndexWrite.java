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

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * write aspects of index.
 * 
 * @author marcus
 *
 * @param <C>
 * @param <V>
 * @param <K>
 */
public interface IndexWrite<C,V,K> {

	void insertIndex(C name,V value, K pkValue) throws ConnectionException;
	
	void updateIndex(C name,V value, V oldValue,  K pkValue) throws ConnectionException;
	
	void removeIndex(C name, V value, K pkValue) throws ConnectionException;
}

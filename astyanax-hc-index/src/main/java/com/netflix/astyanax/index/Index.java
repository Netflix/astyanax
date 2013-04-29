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
 * High cardinality index utility.
 *  
 * I broke it off into a read and write interface, if the interface were ever exposed
 * at the layer of the query or batch mutation level.
 * The other major reason for this is that a mutator does not need to be provided 
 * for query.
 * 
 *  
 * 
 * @author marcus
 *
 * @param <C> - the name of the column to be indexed
 * @param <V> - the value to be indexed 
 * @param <K> - the primary/row key of your referencing CF (reverse look up key)
 */
public interface Index<C,V,K> extends IndexRead<C,V,K>, IndexWrite<C,V,K> {

	
			
	/*
	 * Administrative / expensive operations 
	 */
	
	void buildIndex(String targetCF,C name, Class<K> keyType) throws ConnectionException;
	
	void deleteIndex(String targetCF,C name) throws ConnectionException;
	
	
	
}

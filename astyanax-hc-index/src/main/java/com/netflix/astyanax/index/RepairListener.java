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

/**
 * An event that will be spawned when a repair condition occurs 
 * 
 * @author marcus
 *
 * @param <K>
 * @param <C>
 * @param <V>
 * 
 */
public interface RepairListener <K, C, V> {

	public void onRepair(IndexMapping<C , V> mapping, K key);
	
	
}
